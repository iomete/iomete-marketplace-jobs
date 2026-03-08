import textwrap

from bundle_asset_typesense_sync.config import SyncSettings, chunked


def test_chunked_splits_iterable_into_equal_batches() -> None:
    batches = list(chunked(["a", "b", "c", "d", "e"], 2))
    assert batches == [["a", "b"], ["c", "d"], ["e"]]


def test_chunked_handles_empty_iterable() -> None:
    assert list(chunked([], 3)) == []


def test_from_config_file_loads_settings(tmp_path) -> None:
    conf = tmp_path / "application.conf"
    conf.write_text(
        textwrap.dedent(
            """
            bundle_asset_typesense_sync {
              db {
                host = "db-host"
                port = 5433
                user = "db-user"
                password = "db-pass"
                name = "iam_db"
              }
              cluster_service {
                base_url = "http://cluster.local"
                token = "cluster-token"
              }
              sql_service {
                base_url = "http://sql.local"
                token = "sql-token"
              }
              typesense {
                base_url = "http://typesense.local"
                api_key = "typesense-key"
                collection = "bundle_assets"
                timeout_seconds = 5.0
              }
              batch_size = 200
              http_timeout_seconds = 2.5
            }
            """,
        ).strip(),
    )

    settings = SyncSettings.from_config_file(str(conf))

    assert settings.db.host == "db-host"
    assert settings.db.port == 5433
    assert str(settings.cluster_service.base_url) == "http://cluster.local/"
    assert settings.cluster_service.token == "cluster-token"
    assert settings.batch_size == 200
    assert settings.http_timeout_seconds == 2.5
