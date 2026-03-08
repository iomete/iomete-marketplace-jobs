from __future__ import annotations

import argparse
import logging
import sys

from bundle_asset_typesense_sync import BundleAssetSyncJob
from bundle_asset_typesense_sync.config import SyncSettings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync bundle assets into Typesense.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform all read operations but skip writing to Typesense.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    settings = SyncSettings.from_config_file()
    job = BundleAssetSyncJob(settings)
    job.run(dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
