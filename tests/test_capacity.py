from concurrent.futures import ThreadPoolExecutor
from io import BytesIO

from strata.runtime.errors import VaultError
from strata.runtime.vault import Vault
from support import Clock, Scenario, manifest, sha


class CapacityAccounting(Scenario):
    def setUp(self):
        super().setUp()
        # 受 限对象盘：只有 20 字节可承诺。
        self.limited = Vault(self.root / "limited", self.clock, capacity_bytes=20)
        self.limited.catalog.create_collection("ridge", 60)

    def open_limited(self, token, entries, *, parent=0, lifetime=60):
        return self.limited.deposits.open("ridge", token, parent, lifetime, manifest(entries))

    def upload_limited(self, deposit, data):
        return self.limited.deposits.receive(deposit, sha(data), BytesIO(data))

    def test_report_tracks_physical_reserved_reclaimable_and_committable(self):
        report = self.limited.capacity.report()
        self.assertEqual(report["capacity_bytes"], 20)
        self.assertEqual(report["physical_bytes"], 0)
        self.assertEqual(report["reserved_bytes"], 0)
        self.assertEqual(report["reclaimable_bytes"], 0)
        self.assertEqual(report["reclaim_candidate_bytes"], 0)
        self.assertEqual(report["committable_bytes"], 20)

        opened = self.open_limited("t1", {"f": [b"aaaaa", b"bbb"]})
        report = self.limited.capacity.report()
        self.assertEqual((report["reserved_bytes"], report["reserved_parts"]), (8, 2))
        self.assertEqual((report["physical_bytes"], report["committed_bytes"]), (0, 8))
        self.assertEqual(report["committable_bytes"], 12)
        self.assertEqual(opened["status"], "open")

    def test_whole_application_is_rejected_without_partial_commitment(self):
        self.open_limited("t1", {"f": [b"a" * 12]})
        before = self.limited.capacity.report()
        with self.assertRaises(VaultError) as caught:
            # 新增 9 字节会使承诺达到 21，超过 20。
            self.open_limited("t2", {"g": [b"b" * 9]})
        error = caught.exception
        self.assertEqual(error.status, 409)
        self.assertEqual(error.details["required_bytes"], 9)
        self.assertEqual(error.details["committable_bytes"], 8)
        self.assertEqual(error.details["capacity_bytes"], 20)
        # 拒绝后没有任何半份承诺，可用容量保持不变，也没有多余会话。
        self.assertEqual(self.limited.capacity.report(), before)
        with self.limited.db.transaction(write=False) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM deposits").fetchone()[0], 1)

    def test_shared_missing_part_is_reserved_once_but_protected_while_any_commitment_remains(self):
        first = self.open_limited("t1", {"f": [b"aaaaa", b"bbbbb"]})  # 10
        second = self.open_limited("t2", {"g": [b"aaaaa", b"ccc"]})    # 仅新增 3
        report = self.limited.capacity.report()
        self.assertEqual((report["reserved_bytes"], report["reserved_parts"]), (13, 3))
        self.assertEqual(report["committable_bytes"], 7)

        # 同样缺 aaaaa 的第三个会话不增加任何承诺。
        third = self.open_limited("t3", {"h": [b"aaaaa"]})
        self.assertEqual(self.limited.capacity.report()["reserved_bytes"], 13)

        # 上传共享分片后，承诺转为物理占用，提交总量不变。
        self.upload_limited(first["id"], b"aaaaa")
        report = self.limited.capacity.report()
        self.assertEqual((report["physical_bytes"], report["reserved_bytes"],
                          report["committed_bytes"]), (5, 8, 13))
        # 另一个会话仍依赖该对象：它不是可回收对象。
        self.assertEqual(report["reclaim_candidate_bytes"], 0)
        self.assertEqual(third["status"], "open")

    def test_session_retry_never_enlarges_the_commitment(self):
        entries = {"f": [b"aaaaa", b"bbb"]}
        opened = self.open_limited("t1", entries)
        for _ in range(3):
            again = self.open_limited("t1", entries)
            self.assertEqual(again["id"], opened["id"])
        report = self.limited.capacity.report()
        self.assertEqual((report["reserved_bytes"], report["reserved_parts"]), (8, 2))

    def test_settlement_charges_each_object_once_even_when_receipt_is_lost(self):
        opened = self.open_limited("t1", {"f": [b"aaaaa", b"bbb"]})
        first = self.upload_limited(opened["id"], b"aaaaa")
        self.assertTrue(first["stored"])
        replay = self.upload_limited(opened["id"], b"aaaaa")
        self.assertTrue(replay["duplicate"])
        self.assertFalse(replay["stored"])
        report = self.limited.capacity.report()
        self.assertEqual(report["physical_bytes"], 5)
        self.assertEqual(report["physical_object_count"], 1)
        self.assertEqual(report["reserved_bytes"], 3)
        self.assertEqual(report["committed_bytes"], 8)

    def test_cancel_and_expiry_release_only_that_sessions_responsibility(self):
        first = self.open_limited("t1", {"f": [b"aaaaa", b"bbb"]})     # 8
        second = self.open_limited("t2", {"g": [b"aaaaa", b"cc"]})     # 仅新增 2
        self.upload_limited(first["id"], b"aaaaa")
        self.limited.deposits.cancel(first["id"])
        report = self.limited.capacity.report()
        # t1 的 bbb 占位被释放；t2 仍缺 cc（2），aaaaa 转为回收候选但仍占物理。
        self.assertEqual((report["reserved_bytes"], report["physical_bytes"]), (2, 5))
        self.assertEqual(report["reclaim_candidate_bytes"], 5)
        self.assertEqual(report["reclaimable_bytes"], 0)

        expires = self.open_limited("t3", {"k": [b"dd"]}, lifetime=60)
        self.clock.advance(61)
        expired = self.limited.deposits.expire(10)
        # t2 也在同一时刻到期；它们的占位都被释放。
        self.assertIn(expires["id"], expired)
        report = self.limited.capacity.report()
        self.assertEqual(report["reserved_bytes"], 0)

    def test_marked_bytes_still_occupy_space_and_sweep_frees_only_after_unlink(self):
        opened = self.open_limited("t1", {"f": [b"aaaaa"]})
        self.upload_limited(opened["id"], b"aaaaa")
        self.limited.deposits.cancel(opened["id"])
        # 清扫预告不等于腾空：仍是物理占用，也不是可承诺余量。
        self.assertEqual(self.limited.custody.mark(10), [sha(b"aaaaa")])
        report = self.limited.capacity.report()
        self.assertEqual((report["physical_bytes"], report["reclaimable_bytes"]), (5, 5))
        self.assertEqual(report["reclaim_candidate_bytes"], 0)
        self.assertEqual(report["committable_bytes"], 15)
        # 真正清扫后才释放物理空间。
        self.limited.custody.sweep(10)
        report = self.limited.capacity.report()
        self.assertEqual((report["physical_bytes"], report["reclaimable_bytes"],
                          report["committable_bytes"]), (0, 0, 20))

    def test_reservation_self_heals_when_a_needed_object_is_reclaimed(self):
        first = self.open_limited("t1", {"f": [b"aaaaa", b"bbb"]})
        second = self.open_limited("t2", {"g": [b"aaaaa", b"cc"]})
        self.upload_limited(first["id"], b"aaaaa")
        self.limited.deposits.cancel(first["id"])
        self.limited.custody.mark(10)
        # mark 后开放会话 t2 仍需要 aaaaa：预留自动重新计入，总量不超过原承诺。
        marked_report = self.limited.capacity.report()
        self.assertEqual((marked_report["physical_bytes"], marked_report["reserved_bytes"],
                          marked_report["committed_bytes"]), (5, 7, 12))
        self.limited.custody.sweep(10)
        swept = self.limited.capacity.report()
        self.assertEqual((swept["physical_bytes"], swept["reserved_bytes"],
                          swept["committed_bytes"]), (0, 7, 7))
        # t2 仍能补齐字节并发布。
        self.upload_limited(second["id"], b"aaaaa")
        self.upload_limited(second["id"], b"cc")
        self.limited.catalog.publish(second["id"])
        report = self.limited.capacity.report()
        self.assertEqual((report["physical_bytes"], report["reserved_bytes"]), (7, 0))
        self.assertEqual(report["reclaim_candidate_bytes"], 0)

    def test_published_revision_keeps_objects_protected(self):
        opened = self.open_limited("t1", {"f": [b"aaaaa", b"bbb"]})
        self.upload_limited(opened["id"], b"aaaaa")
        self.upload_limited(opened["id"], b"bbb")
        self.limited.catalog.publish(opened["id"])
        report = self.limited.capacity.report()
        self.assertEqual((report["physical_bytes"], report["reserved_bytes"],
                          report["reclaim_candidate_bytes"]), (8, 0, 0))
        self.assertEqual(self.limited.custody.mark(10), [])

    def test_capacity_limit_and_accounting_survive_restart(self):
        self.open_limited("t1", {"f": [b"aaaaa"]})
        restarted = Vault(self.root / "limited", self.clock)
        self.assertEqual(restarted.capacity.limit(), 20)
        report = restarted.capacity.report()
        self.assertEqual((report["reserved_bytes"], report["committable_bytes"]), (5, 15))

    def test_unlimited_vault_reports_null_committable_and_never_blocks(self):
        self.vault.catalog.create_collection("open", 60)
        self.vault.deposits.open("open", "big", 0, 60, manifest({"f": [b"x" * 100]}))
        report = self.vault.capacity.report()
        self.assertIsNone(report["capacity_bytes"])
        self.assertIsNone(report["committable_bytes"])
        self.assertEqual(report["reserved_bytes"], 100)

    def test_adjusting_limit_does_not_disturb_existing_commitments(self):
        self.open_limited("t1", {"f": [b"a" * 15]})
        tighter = self.limited.capacity.set_limit(10)
        self.assertEqual((tighter["capacity_bytes"], tighter["reserved_bytes"],
                          tighter["committed_bytes"], tighter["committable_bytes"]),
                         (10, 15, 15, 0))
        removed = self.limited.capacity.set_limit(None)
        self.assertIsNone(removed["capacity_bytes"])
        self.assertIsNone(removed["committable_bytes"])
        # 既有承诺仍然保留。
        self.assertEqual(self.limited.capacity.report()["reserved_bytes"], 15)

    def test_concurrent_sessions_are_serialized_without_oversubscription(self):
        from threading import Barrier
        barrier = Barrier(4)
        # 每个分片固定 7 字节且互不共享。
        parts = {f"w{i}": [bytes([0x10 + i]) * 7] for i in range(4)}

        def open_session(index):
            token = f"w{index}"
            barrier.wait(timeout=5)
            try:
                self.limited.deposits.open(
                    "ridge", token, 0, 60, manifest({token: parts[token]}),
                )
                return "accepted"
            except VaultError:
                return "rejected"

        with ThreadPoolExecutor(max_workers=4) as workers:
            outcomes = list(workers.map(open_session, range(4)))
        # 每个会话独占 7 字节：20 字节只能容纳 2 个，其余整单被拒。
        self.assertEqual(sorted(outcomes), ["accepted", "accepted", "rejected", "rejected"])
        self.assertEqual(self.limited.capacity.report()["committed_bytes"], 14)


class CapacityContract(Scenario):
    def test_invalid_limit_values_are_rejected(self):
        for bad in (0, -1, "20", 1.5, True):
            with self.subTest(bad=bad):
                with self.assertRaises(VaultError):
                    self.vault.capacity.set_limit(bad)

    def test_digest_with_conflicting_length_is_rejected_at_reservation(self):
        limited = Vault(self.root / "limited", self.clock, capacity_bytes=100)
        limited.catalog.create_collection("ridge", 60)
        opened = limited.deposits.open("ridge", "t1", 0, 60, manifest({"f": [b"abc"]}))
        limited.deposits.receive(opened["id"], sha(b"abc"), BytesIO(b"abc"))
        # 用已存在对象的摘要声明不同长度：清单一致性先拦截，不会产生承诺。
        forged = manifest({"g": [b"abcd"]})
        forged["files"][0]["parts"][0]["sha256"] = sha(b"abc")
        with self.assertRaisesRegex(VaultError, "length|digest"):
            limited.deposits.open("ridge", "t2", 0, 60, forged)
        self.assertEqual(limited.capacity.report()["reserved_bytes"], 0)
