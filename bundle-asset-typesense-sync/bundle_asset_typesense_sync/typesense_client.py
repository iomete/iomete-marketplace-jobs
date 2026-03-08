from __future__ import annotations

import json
import logging
from typing import Iterable

import requests

from .config import SyncSettings

logger = logging.getLogger(__name__)


class TypesenseClient:
    def __init__(self, settings: SyncSettings):
        self._settings = settings
        self._session = requests.Session()
        self._session.headers.update({"X-TYPESENSE-API-KEY": settings.typesense.api_key})

    def _url(self, path: str) -> str:
        return f"{self._settings.typesense.base_url}{path}"

    def ensure_collection(self) -> None:
        schema = self._collection_schema()
        url = self._url(f"/collections/{self._settings.typesense.collection}")
        resp = self._session.get(url, timeout=self._settings.typesense.timeout_seconds)
        if resp.status_code == 200:
            updates = self._fields_requiring_update(resp.json(), schema)
            if updates:
                self._patch_collection_fields(updates)
            return
        if resp.status_code != 404:
            resp.raise_for_status()

        logger.info("Creating Typesense collection %s", self._settings.typesense.collection)
        create_resp = self._session.post(
            self._url("/collections"),
            json=schema,
            timeout=self._settings.typesense.timeout_seconds,
        )
        create_resp.raise_for_status()

    def _collection_schema(self) -> dict:
        return {
            "name": self._settings.typesense.collection,
            "token_separators": [" ", "_", "-", ".", ":"],
            "fields": [
                {"name": "bundle_id", "type": "string", "facet": True},
                {"name": "asset_id", "type": "string"},
                {
                    "name": "asset_type",
                    "type": "string",
                    "facet": True,
                    "infix": True,
                    "sort": True,
                },
                {"name": "asset_name", "type": "string", "infix": True, "sort": True},
            ],
        }

    def _fields_requiring_update(self, existing: dict, desired: dict) -> list[dict]:
        updates: list[dict] = []
        existing_fields = {field.get("name"): field for field in existing.get("fields", [])}
        for field in desired.get("fields", []):
            name = field.get("name")
            current = existing_fields.get(name)
            if not current:
                updates.append(field)
                continue
            for key, value in field.items():
                if current.get(key) != value:
                    updates.append({"name": name, "drop": True})
                    updates.append(field)
                    break
        return updates

    def _patch_collection_fields(self, fields: list[dict]) -> None:
        if not fields:
            return
        logger.info("Updating Typesense collection %s schema", self._settings.typesense.collection)
        patch_resp = self._session.patch(
            self._url(f"/collections/{self._settings.typesense.collection}"),
            json={"fields": fields},
            timeout=self._settings.typesense.timeout_seconds,
        )
        patch_resp.raise_for_status()

    def delete_bundle_documents(self, bundle_id: str) -> None:
        resp = self._session.delete(
            self._url(f"/collections/{self._settings.typesense.collection}/documents"),
            params={"filter_by": f"bundle_id:={bundle_id}"},
            timeout=self._settings.typesense.timeout_seconds,
        )
        if resp.status_code not in (200, 404):
            resp.raise_for_status()

    def import_documents(self, documents: Iterable[dict]) -> None:
        docs = list(documents)
        if not docs:
            return

        payload = "\n".join(json.dumps(doc, separators=(",", ":")) for doc in docs)
        resp = self._session.post(
            self._url(f"/collections/{self._settings.typesense.collection}/documents/import"),
            params={"action": "create"},
            data=payload,
            headers={"Content-Type": "text/plain"},
            timeout=self._settings.typesense.timeout_seconds,
        )
        resp.raise_for_status()
        failures = self._parse_import_response(resp.text)
        if failures:
            for failure in failures[:5]:
                logger.error(
                    "Typesense import failure (id=%s): %s",
                    failure.get("id"),
                    failure.get("error"),
                )
            raise RuntimeError(f"Typesense import failed for {len(failures)} documents")

    @staticmethod
    def _parse_import_response(body: str) -> list[dict]:
        failures: list[dict] = []
        for line in body.splitlines():
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                failures.append({"id": None, "error": f"Invalid response line: {line}"})
                continue
            if not entry.get("success"):
                doc = entry.get("document") or {}
                failures.append(
                    {
                        "id": doc.get("id"),
                        "error": entry.get("error"),
                    },
                )
        return failures

    @staticmethod
    def build_document(
        bundle_id: str,
        asset_type: str,
        asset_id: str,
        asset_name: str,
    ) -> dict:
        return {
            "id": f"{bundle_id}_{asset_type}_{asset_id}",
            "bundle_id": bundle_id,
            "asset_type": asset_type,
            "asset_id": asset_id,
            "asset_name": asset_name,
        }
