import pytest

from bundle_asset_typesense_sync.config import (
    DatabaseSettings,
    HttpServiceSettings,
    SyncSettings,
    TypesenseSettings,
)


@pytest.fixture()
def sync_settings() -> SyncSettings:
    return SyncSettings(
        db=DatabaseSettings(dsn="postgresql://user:pass@localhost:5432/iam"),
        cluster_service=HttpServiceSettings(base_url="http://cluster.local"),
        sql_service=HttpServiceSettings(base_url="http://sql.local"),
        typesense=TypesenseSettings(
            base_url="http://typesense.local",
            api_key="secret",
            collection="bundle_assets",
            timeout_seconds=1.0,
        ),
        batch_size=2,
        http_timeout_seconds=1.0,
    )
