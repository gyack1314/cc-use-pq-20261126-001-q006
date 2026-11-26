"""容量准备与入库结算。

同一份字节对象只计算一次：物理占用按 ``objects`` 行统计，有效承诺按开放会话仍
缺少的去重分片统计。待回收对象虽然被预告删除，但在真正清扫前继续占用物理空间，
也不能被新申请当作余量。所有数字与接收决策使用同一套事务内谓词得出。
"""

from sqlite3 import Connection

from strata.runtime.database import Database
from strata.runtime.errors import conflict, invalid
from strata.runtime.events import append

# 与 preservation.custody 保持一致：既无活版本引用、也没有任何接收收据的对象才
# 可能进入回收流程。承诺所依赖的分片始终有接收收据，因此永远不会被当作余量。
UNREFERENCED = """
    NOT EXISTS (
        SELECT 1 FROM revision_parts p JOIN revisions r
        ON p.collection=r.collection AND p.number=r.number
        WHERE p.digest=o.digest AND r.retired_at IS NULL
    )
    AND NOT EXISTS (SELECT 1 FROM received_parts p WHERE p.digest=o.digest)
"""

CAPACITY_KEY = "object_capacity_bytes"


def open_deposit_predicate(now: int) -> str:
    # 与 transfer.deposits.effective_state 一致：expires_at<=now 才算到期。
    return f"d.status='open' AND d.expires_at>={now}"


class Capacity:
    def __init__(self, db: Database):
        self.db = db

    def limit(self) -> int | None:
        with self.db.transaction(write=False) as connection:
            return self._limit(connection)

    @staticmethod
    def _limit(connection: Connection) -> int | None:
        row = connection.execute("SELECT value FROM settings WHERE key=?", (CAPACITY_KEY,)).fetchone()
        return None if row is None else row["value"]

    def set_limit(self, value: int | None) -> dict:
        if value is not None and (type(value) is not int or value < 1):
            raise invalid("Capacity limit must be a positive integer or null")
        with self.db.transaction() as connection:
            now = self.db.now()
            if value is None:
                connection.execute("DELETE FROM settings WHERE key=?", (CAPACITY_KEY,))
            else:
                connection.execute(
                    "INSERT INTO settings(key,value) VALUES(?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (CAPACITY_KEY, value),
                )
            append(connection, None, "capacity_limit_changed", CAPACITY_KEY, now,
                   capacity_bytes=value)
            return self.report(connection=connection)

    def report(self, *, connection: Connection | None = None) -> dict:
        if connection is None:
            with self.db.transaction(write=False) as own:
                return self.report(connection=own)
        now = self.db.now()
        physical = connection.execute(
            """SELECT COUNT(*) AS object_count,COALESCE(SUM(size),0) AS bytes
               FROM objects""",
        ).fetchone()
        # 待回收：已经过 mark 预告、但 sweep 尚未真正腾空的对象。它们继续占用物理空间。
        reclaimable = connection.execute(
            "SELECT COUNT(*) AS object_count,COALESCE(SUM(size),0) AS bytes "
            "FROM objects WHERE sweep_state='marked'",
        ).fetchone()
        # 回收候选：仍为 live 但已无任何保护引用，等待维护人员 mark；此刻还不能当作余量。
        candidates = connection.execute(
            "SELECT COUNT(*) AS object_count,COALESCE(SUM(size),0) AS bytes "
            "FROM objects o WHERE sweep_state='live' AND " + UNREFERENCED,
        ).fetchone()
        reserved = self._reservation_totals(connection, now)
        limit = self._limit(connection)
        committed = physical["bytes"] + reserved["bytes"]
        available = None if limit is None else max(0, limit - committed)
        return {
            "capacity_bytes": limit,
            "physical_bytes": physical["bytes"],
            "physical_object_count": physical["object_count"],
            "reserved_bytes": reserved["bytes"],
            "reserved_parts": reserved["count"],
            "committed_bytes": committed,
            "reclaimable_bytes": reclaimable["bytes"],
            "reclaimable_object_count": reclaimable["object_count"],
            "reclaim_candidate_bytes": candidates["bytes"],
            "reclaim_candidate_object_count": candidates["object_count"],
            "committable_bytes": available,
        }

    def _reservation_totals(self, connection: Connection, now: int) -> dict:
        row = connection.execute(
            f"""SELECT COUNT(*) AS count,COALESCE(SUM(size),0) AS bytes FROM (
                    SELECT e.digest,MAX(e.size) AS size FROM expected_parts e
                    JOIN deposits d ON d.id=e.deposit
                    WHERE {open_deposit_predicate(now)}
                    AND NOT EXISTS (
                        SELECT 1 FROM objects o WHERE o.digest=e.digest AND o.sweep_state='live'
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM received_parts p
                        WHERE p.deposit=e.deposit AND p.digest=e.digest
                    )
                    GROUP BY e.digest
                )""",
        ).fetchone()
        return {"count": row["count"], "bytes": row["bytes"]}

    def reservation_for(self, connection: Connection, parts: dict[str, int], now: int) -> dict:
        """新会话需要追加的承诺：尚未落盘为活对象、且未被其他有效会话承诺的去重分片。

        在插入任何会话行之前调用，容量不足时整个事务回滚，不会留下半份承诺。
        """
        if not parts:
            return {"parts": [], "bytes": 0}
        rows = connection.execute(
            f"""SELECT e.digest,MAX(e.size) AS size FROM expected_parts e
                JOIN deposits d ON d.id=e.deposit
                WHERE {open_deposit_predicate(now)} AND e.digest IN ({",".join("?" for _ in parts)})
                GROUP BY e.digest""",
            tuple(parts),
        ).fetchall()
        already = {row["digest"]: row["size"] for row in rows}
        requested = []
        for digest, size in parts.items():
            item = connection.execute(
                "SELECT size,sweep_state FROM objects WHERE digest=?", (digest,),
            ).fetchone()
            if item is not None and item["size"] != size:
                raise invalid("Part length differs from known object length", digest=digest)
            if item is not None and item["sweep_state"] == "live":
                continue
            shared = already.get(digest)
            if shared is not None:
                if shared != size:
                    raise invalid("Part length differs from a committed object length", digest=digest)
                continue
            requested.append({"digest": digest, "size": size})
        return {"parts": requested, "bytes": sum(item["size"] for item in requested)}

    def ensure_room(self, connection: Connection, required: int) -> None:
        """整单容量检查：不足时拒绝整个申请，不留下任何部分承诺。"""
        limit = self._limit(connection)
        if limit is None:
            return
        snapshot = self.report(connection=connection)
        if snapshot["committed_bytes"] + required > limit:
            raise conflict(
                "Not enough object storage capacity for the whole deposit",
                required_bytes=required,
                committable_bytes=snapshot["committable_bytes"],
                capacity_bytes=limit,
                physical_bytes=snapshot["physical_bytes"],
                reserved_bytes=snapshot["reserved_bytes"],
            )
