"""启动档案服务并使用指定的本地库目录。"""

import argparse
from pathlib import Path

import uvicorn

from strata.api import create_app


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=Path("vault-data"), help="Local catalog and object directory")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--capacity-bytes", type=int, default=None,
                        help="Limited object-disk capacity in bytes; omit for no configured limit")
    options = parser.parse_args()
    uvicorn.run(create_app(options.data, capacity_bytes=options.capacity_bytes),
                host=options.host, port=options.port)


if __name__ == "__main__":
    main()
