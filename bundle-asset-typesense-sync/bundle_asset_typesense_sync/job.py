from __future__ import annotations

import logging
from collections import defaultdict
from typing import Iterable, Mapping

from .asset_fetchers import AssetNameResolver
from .config import BundleAssetRecord, SyncSettings
from .db import IamRepository
from .typesense_client import TypesenseClient

logger = logging.getLogger(__name__)


class BundleAssetSyncJob:
    def __init__(self, settings: SyncSettings):
        self._settings = settings
        self._repo = IamRepository(settings)
        self._resolver = AssetNameResolver(settings, self._repo)
        self._typesense = TypesenseClient(settings)

    def run(
        self,
        *,
        dry_run: bool = False,
    ) -> None:
        self._typesense.ensure_collection()
        with self._repo.connection() as conn:
            processed_bundles = 0
            for bundle_id in self._repo.iter_active_bundle_ids(conn):
                try:
                    processed_bundles += self._process_single_bundle(
                        conn,
                        bundle_id,
                        dry_run=dry_run,
                    )
                except Exception as exc:
                    logger.exception("Failed to index bundle %s: %s", bundle_id, exc)

            if processed_bundles == 0:
                logger.info("No bundle assets returned from IAM")
            else:
                logger.info("Sync complete: %d bundles processed", processed_bundles)

    def _process_single_bundle(
        self,
        conn,
        bundle_id: str,
        *,
        dry_run: bool,
    ) -> int:
        records = list(self._repo.stream_bundle_assets(conn, [bundle_id]))
        if not records:
            return 0

        name_map = self._resolver.resolve(
            conn,
            ((record.asset_type, record.asset_id) for record in records),
        )

        self._process_bundle(bundle_id, records, name_map, dry_run=dry_run)
        return 1

    def _process_bundle(
        self,
        bundle_id: str,
        assets: Iterable[BundleAssetRecord],
        name_map: Mapping[tuple[str, str], str],
        *,
        dry_run: bool,
    ) -> None:
        documents = []
        missing = 0
        for asset in assets:
            key = (asset.asset_type, asset.asset_id)
            asset_name = name_map.get(key)
            if not asset_name:
                missing += 1
                continue
            documents.append(
                self._typesense.build_document(
                    bundle_id=bundle_id,
                    asset_type=asset.asset_type,
                    asset_id=asset.asset_id,
                    asset_name=asset_name,
                ),
            )

        logger.info(
            "Bundle %s: %d assets (skipped %d missing names)",
            bundle_id,
            len(documents),
            missing,
        )

        if dry_run:
            return

        self._typesense.delete_bundle_documents(bundle_id)
        self._typesense.import_documents(documents)
