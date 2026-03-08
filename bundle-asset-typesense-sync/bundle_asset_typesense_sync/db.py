from __future__ import annotations

from contextlib import contextmanager
from typing import Dict, Iterable, Iterator, Sequence

import psycopg

from .config import BundleAssetRecord, SyncSettings


class IamRepository:
    def __init__(self, settings: SyncSettings):
        self._settings = settings

    @contextmanager
    def connection(self) -> Iterator[psycopg.Connection]:
        conn = psycopg.connect(self._settings.db.resolved_dsn)
        try:
            yield conn
        finally:
            conn.close()

    def iter_active_bundle_ids(self, conn: psycopg.Connection) -> Iterator[str]:
        query = "SELECT id::text FROM bundle WHERE is_archived = false"
        with conn.cursor() as cur:
            cur.execute(query)
            while True:
                rows = cur.fetchmany(self._settings.batch_size)
                if not rows:
                    break
                for (bundle_id,) in rows:
                    yield bundle_id

    def stream_bundle_assets(
        self,
        conn: psycopg.Connection,
        bundle_ids: Sequence[str] | None = None,
    ) -> Iterator[BundleAssetRecord]:
        params: Dict[str, object] = {}
        bundle_clause = ""
        if bundle_ids:
            bundle_clause = "AND ba.bundle_id = ANY(%(bundle_ids)s)"
            params["bundle_ids"] = bundle_ids

        query = f"""
            SELECT ba.bundle_id::text, ba.asset_type, ba.asset_id
            FROM bundle_asset ba
            JOIN bundle b ON b.id = ba.bundle_id
            WHERE b.is_archived = false
            {bundle_clause}
        """
        with conn.cursor(name="bundle_asset_cursor") as cur:
            cur.itersize = self._settings.batch_size
            cur.execute(query, params)
            for bundle_id, asset_type, asset_id in cur:
                yield BundleAssetRecord(
                    bundle_id=bundle_id,
                    asset_type=asset_type,
                    asset_id=asset_id,
                )

    def fetch_domain_names(
        self,
        conn: psycopg.Connection,
        ids: Iterable[str],
    ) -> Dict[str, str]:
        ids = list(ids)
        if not ids:
            return {}
        query = "SELECT id::text, name FROM domain WHERE id = ANY(%s)"
        with conn.cursor() as cur:
            cur.execute(query, (ids,))
            return {row[0]: row[1] for row in cur.fetchall()}
