from __future__ import annotations

import json
import logging
from typing import Dict, Iterable, Mapping

import requests

from .config import SyncSettings, chunked
from .db import IamRepository

logger = logging.getLogger(__name__)


class AssetFetcher:
    def __init__(
        self,
        *,
        session: requests.Session,
        url: str,
        headers: Mapping[str, str],
        timeout: float,
        batch_size: int = 200,
    ):
        self._session = session
        self._url = url
        self._headers = dict(headers)
        self._timeout = timeout
        self._batch_size = batch_size

    def _log_failure(self, asset_type: str, exc: Exception) -> None:
        logger.warning("Failed to fetch %s assets from %s: %s", asset_type, self._url, exc)


class GetQueryFetcher(AssetFetcher):
    def fetch(self, asset_type: str, ids: Iterable[str]) -> Dict[str, str]:
        result: Dict[str, str] = {}
        for batch in chunked(ids, self._batch_size):
            params = [("ids", value) for value in batch]
            try:
                resp = self._session.get(
                    self._url,
                    params=params,
                    headers=self._headers,
                    timeout=self._timeout,
                )
                resp.raise_for_status()
                for entry in resp.json():
                    entry_id = entry.get("id")
                    if not entry_id:
                        continue
                    name = entry.get("name") or entry.get("namespace")
                    if name:
                        result[entry_id] = name
            except Exception as exc:
                self._log_failure(asset_type, exc)
        return result


class PostJsonFetcher(AssetFetcher):
    def fetch(self, asset_type: str, ids: Iterable[str]) -> Dict[str, str]:
        result: Dict[str, str] = {}
        for batch in chunked(ids, self._batch_size):
            try:
                resp = self._session.post(
                    self._url,
                    data=json.dumps(batch),
                    headers={**self._headers, "Content-Type": "application/json"},
                    timeout=self._timeout,
                )
                resp.raise_for_status()
                for entry in resp.json():
                    entry_id = entry.get("id")
                    name = entry.get("name")
                    if entry_id and name:
                        result[entry_id] = name
            except Exception as exc:
                self._log_failure(asset_type, exc)
        return result


class AssetNameResolver:
    def __init__(
        self,
        settings: SyncSettings,
        repo: IamRepository,
    ):
        self._settings = settings
        self._repo = repo
        self._session = requests.Session()
        self._fetchers: Dict[str, AssetFetcher] = {
            "COMPUTE": GetQueryFetcher(
                session=self._session,
                url=f"{settings.cluster_service.base_url}/internal/v1/compute",
                headers=settings.cluster_service.headers(),
                timeout=settings.http_timeout_seconds,
            ),
            "SPARK_JOB": PostJsonFetcher(
                session=self._session,
                url=f"{settings.cluster_service.base_url}/internal/v1/spark/jobs",
                headers=settings.cluster_service.headers(),
                timeout=settings.http_timeout_seconds,
            ),
            "STORAGE_CONFIG": GetQueryFetcher(
                session=self._session,
                url=f"{settings.cluster_service.base_url}/internal/v1/storage-configs",
                headers=settings.cluster_service.headers(),
                timeout=settings.http_timeout_seconds,
            ),
            "WORKSPACE": GetQueryFetcher(
                session=self._session,
                url=f"{settings.sql_service.base_url}/internal/v1/sql/workspaces",
                headers=settings.sql_service.headers(),
                timeout=settings.http_timeout_seconds,
            ),
            "JUPYTER_CONTAINER": GetQueryFetcher(
                session=self._session,
                url=f"{settings.cluster_service.base_url}/internal/v1/jupyter-containers",
                headers=settings.cluster_service.headers(),
                timeout=settings.http_timeout_seconds,
            ),
            "NAMESPACE": GetQueryFetcher(
                session=self._session,
                url=f"{settings.cluster_service.base_url}/internal/v1/namespaces/mappings",
                headers=settings.cluster_service.headers(),
                timeout=settings.http_timeout_seconds,
            ),
            "SCHEDULE": GetQueryFetcher(
                session=self._session,
                url=f"{settings.cluster_service.base_url}/internal/v1/schedules",
                headers=settings.cluster_service.headers(),
                timeout=settings.http_timeout_seconds,
            ),
            "EVENT_STREAM": GetQueryFetcher(
                session=self._session,
                url=f"{settings.cluster_service.base_url}/internal/v1/event-streams",
                headers=settings.cluster_service.headers(),
                timeout=settings.http_timeout_seconds,
            ),
            "VAULT_CONFIG": GetQueryFetcher(
                session=self._session,
                url=f"{settings.cluster_service.base_url}/internal/v1/vault-configs",
                headers=settings.cluster_service.headers(),
                timeout=settings.http_timeout_seconds,
            ),
        }

    def resolve(
        self,
        conn,
        assets: Iterable[tuple[str, str]],
    ) -> Dict[tuple[str, str], str]:
        ids_by_type: Dict[str, set[str]] = {}
        for asset_type, asset_id in assets:
            ids_by_type.setdefault(asset_type, set()).add(asset_id)

        resolved: Dict[tuple[str, str], str] = {}

        def _store(asset_type: str, mapping: Dict[str, str]) -> None:
            for asset_id, name in mapping.items():
                resolved[(asset_type, asset_id)] = name

        for asset_type, ids in ids_by_type.items():
            if asset_type == "DOMAIN":
                names = self._repo.fetch_domain_names(conn, ids)
                _store(asset_type, names)
                continue

            fetcher = self._fetchers.get(asset_type)
            if not fetcher:
                logger.info("No fetcher configured for asset type %s", asset_type)
                continue
            mapping = fetcher.fetch(asset_type, ids)
            _store(asset_type, mapping)

        return resolved
