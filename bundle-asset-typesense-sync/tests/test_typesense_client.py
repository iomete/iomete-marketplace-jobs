from __future__ import annotations

from typing import Any

import pytest

from bundle_asset_typesense_sync.config import (
    DatabaseSettings,
    HttpServiceSettings,
    SyncSettings,
    TypesenseSettings,
)
import bundle_asset_typesense_sync.typesense_client as typesense_client


class DummyResponse:
    def __init__(self, status_code: int, payload: dict[str, Any] | None = None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict[str, Any]:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class DummySession:
    def __init__(self, scripted: dict[str, list[DummyResponse]]):
        self._scripted = {method: list(responses) for method, responses in scripted.items()}
        self.headers: dict[str, str] = {}
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def _next(self, method: str) -> DummyResponse:
        responses = self._scripted.get(method)
        if not responses:
            raise AssertionError(f"No scripted response for {method}")
        return responses.pop(0)

    def get(self, url: str, *, timeout: float) -> DummyResponse:
        resp = self._next("get")
        self.calls.append(("get", url, {"timeout": timeout}))
        return resp

    def post(self, url: str, *, json: dict[str, Any], timeout: float) -> DummyResponse:
        resp = self._next("post")
        self.calls.append(("post", url, {"json": json, "timeout": timeout}))
        return resp

    def patch(self, url: str, *, json: dict[str, Any], timeout: float) -> DummyResponse:
        resp = self._next("patch")
        self.calls.append(("patch", url, {"json": json, "timeout": timeout}))
        return resp

    def delete(self, url: str, *, params: dict[str, Any], timeout: float) -> DummyResponse:
        resp = self._next("delete")
        self.calls.append(("delete", url, {"params": params, "timeout": timeout}))
        return resp


@pytest.fixture()
def dummy_session(monkeypatch: pytest.MonkeyPatch):
    created_session: DummySession | None = None

    def factory(scripted: dict[str, list[DummyResponse]]) -> DummySession:
        nonlocal created_session
        created_session = DummySession(scripted)
        return created_session

    # Provide helper to tests via closure
    def _set_session(scripted: dict[str, list[DummyResponse]]) -> DummySession:
        session = factory(scripted)
        monkeypatch.setattr(typesense_client.requests, "Session", lambda: session)
        return session

    return _set_session


def _settings() -> SyncSettings:
    return SyncSettings(
        db=DatabaseSettings(),
        cluster_service=HttpServiceSettings(base_url="http://cluster.local"),
        sql_service=HttpServiceSettings(base_url="http://sql.local"),
        typesense=TypesenseSettings(
            base_url="http://typesense.local",
            api_key="key",
            timeout_seconds=1.0,
            collection="bundle_assets",
        ),
    )


def test_ensure_collection_creates_when_missing(dummy_session) -> None:
    session = dummy_session(
        {
            "get": [DummyResponse(404)],
            "post": [DummyResponse(201)],
        },
    )

    client = typesense_client.TypesenseClient(_settings())
    client.ensure_collection()

    post_calls = [call for call in session.calls if call[0] == "post"]
    assert len(post_calls) == 1
    sent_schema = post_calls[0][2]["json"]
    fields = {field["name"]: field for field in sent_schema["fields"]}
    assert fields["asset_type"]["infix"] is True
    assert fields["asset_type"]["sort"] is True
    assert fields["asset_name"]["sort"] is True


def test_ensure_collection_patches_mismatched_fields(dummy_session) -> None:
    existing_fields = [
        {"name": "bundle_id", "type": "string", "facet": True},
        {"name": "asset_id", "type": "string"},
        {"name": "asset_type", "type": "string", "facet": True},
        {"name": "asset_name", "type": "string", "infix": True},
        {"name": "is_marker", "type": "bool", "facet": True},
    ]
    session = dummy_session(
        {
            "get": [DummyResponse(200, {"fields": existing_fields})],
            "patch": [DummyResponse(200)],
        },
    )

    client = typesense_client.TypesenseClient(_settings())
    client.ensure_collection()

    patch_calls = [call for call in session.calls if call[0] == "patch"]
    assert len(patch_calls) == 1
    fields_payload = patch_calls[0][2]["json"]["fields"]
    # ensure drop then recreate for asset_type and asset_name
    drops = [field for field in fields_payload if field.get("drop")]
    assert any(field["name"] == "asset_type" for field in drops)
    recreated = {field["name"]: field for field in fields_payload if not field.get("drop")}
    assert recreated["asset_type"]["infix"] is True
    assert recreated["asset_name"]["sort"] is True


def test_import_documents_logs_failures(monkeypatch, caplog) -> None:
    session = DummySession(
        {
            "get": [DummyResponse(404)],
            "post": [
                DummyResponse(201),
                DummyResponse(
                    200,
                    payload=None,
                ),
            ],
        },
    )
    monkeypatch.setattr(typesense_client.requests, "Session", lambda: session)
    client = typesense_client.TypesenseClient(_settings())

    # Patch post to return custom text
    def fake_post(*args, **kwargs):
        class Resp:
            status_code = 200

            def raise_for_status(self):
                pass

            text = '{"success":false,"error":"bad","document":{"id":"doc1"}}\n{"success":true}'

        return Resp()

    monkeypatch.setattr(client._session, "post", fake_post)

    with caplog.at_level("ERROR"):
        try:
            client.import_documents([{"id": "doc1"}])
        except RuntimeError as exc:
            assert "Typesense import failed for" in str(exc)
        else:
            pytest.fail("Expected RuntimeError for failed import")
