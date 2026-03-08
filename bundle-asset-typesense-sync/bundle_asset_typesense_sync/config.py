from __future__ import annotations

import os
from functools import cached_property
from pathlib import Path
from typing import Iterable, Optional

from pydantic import BaseModel, Field, HttpUrl
from pyhocon import ConfigFactory, ConfigTree


class DatabaseSettings(BaseModel):
    dsn: Optional[str] = Field(
        default=None,
        description="Full PostgreSQL DSN (postgresql://user:pass@host:port/db).",
    )
    host: str = "localhost"
    port: int = 5432
    user: str = "postgres"
    password: str = "postgres"
    name: str = "postgres"

    @cached_property
    def resolved_dsn(self) -> str:
        if self.dsn:
            return self.dsn
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


class HttpServiceSettings(BaseModel):
    base_url: HttpUrl
    token: Optional[str] = Field(
        default=None,
        description="Optional bearer token sent to the service.",
    )

    def headers(self) -> dict[str, str]:
        if not self.token:
            return {}
        return {"Authorization": f"Bearer {self.token}"}


class TypesenseSettings(BaseModel):
    base_url: HttpUrl
    api_key: str
    collection: str = "bundle_assets"
    timeout_seconds: float = 10.0


class SyncSettings(BaseModel):
    db: DatabaseSettings
    cluster_service: HttpServiceSettings
    sql_service: HttpServiceSettings
    typesense: TypesenseSettings
    batch_size: int = Field(default=1_000, ge=100, le=10_000)
    http_timeout_seconds: float = 10.0

    @staticmethod
    def _env(
        key: str,
        default: Optional[str] = None,
        *,
        required: bool = False,
    ) -> Optional[str]:
        import os

        value = os.getenv(key, default)
        if required and not value:
            raise RuntimeError(f"Environment variable {key} is required")
        return value

    @classmethod
    def from_env(cls) -> "SyncSettings":
        service_token = cls._env("SERVICE_API_TOKEN")
        return cls(
            db=DatabaseSettings(
                dsn=cls._env("IAM_DB_DSN"),
                host=cls._env("IAM_DB_HOST", "localhost"),
                port=int(cls._env("IAM_DB_PORT", "5432")),
                user=cls._env("IAM_DB_USER", "postgres"),
                password=cls._env("IAM_DB_PASSWORD", "postgres"),
                name=cls._env("IAM_DB_NAME", "postgres"),
            ),
            cluster_service=HttpServiceSettings(
                base_url=cls._env("CLUSTER_SERVICE_URL", required=True),  # type: ignore[arg-type]
                token=cls._env("CLUSTER_SERVICE_TOKEN", service_token),
            ),
            sql_service=HttpServiceSettings(
                base_url=cls._env("SQL_SERVICE_URL", required=True),  # type: ignore[arg-type]
                token=cls._env("SQL_SERVICE_TOKEN", service_token),
            ),
            typesense=TypesenseSettings(
                base_url=cls._env("TYPESENSE_URL", required=True),  # type: ignore[arg-type]
                api_key=cls._env("TYPESENSE_API_KEY", required=True),
                collection=cls._env("TYPESENSE_COLLECTION", "bundle_assets"),
                timeout_seconds=float(cls._env("TYPESENSE_TIMEOUT_SECONDS", "10.0")),
            ),
            batch_size=int(cls._env("SYNC_BATCH_SIZE", "1000")),
            http_timeout_seconds=float(cls._env("HTTP_TIMEOUT_SECONDS", "10.0")),
        )

    @classmethod
    def from_config_file(cls, path: Optional[str] = None) -> "SyncSettings":
        config_path = cls._resolve_config_path(path)
        config = ConfigFactory.parse_file(config_path)
        section: ConfigTree | dict = config.get("bundle_asset_typesense_sync", config)

        def _require(key: str) -> dict:
            value = section.get(key)
            if value is None:
                raise RuntimeError(f"Missing '{key}' in config file {config_path}")
            if isinstance(value, ConfigTree):
                return value.as_plain_ordered_dict()
            return dict(value)

        return cls(
            db=DatabaseSettings(**_require("db")),
            cluster_service=HttpServiceSettings(**_require("cluster_service")),
            sql_service=HttpServiceSettings(**_require("sql_service")),
            typesense=TypesenseSettings(**_require("typesense")),
            batch_size=int(section.get("batch_size", 1_000)),
            http_timeout_seconds=float(section.get("http_timeout_seconds", 10.0)),
        )

    @staticmethod
    def _config_candidates(path: Optional[str]) -> Iterable[str]:
        env_path = os.getenv("BUNDLE_ASSET_SYNC_CONFIG")
        candidates = []
        if path:
            candidates.append(path)
        if env_path:
            candidates.append(env_path)
        candidates.extend(
            [
                "/etc/configs/application.conf",
                "application.conf",
            ],
        )
        return [candidate for candidate in candidates if candidate]

    @classmethod
    def _resolve_config_path(cls, path: Optional[str]) -> str:
        for candidate in cls._config_candidates(path):
            if Path(candidate).is_file():
                return candidate
        raise RuntimeError(
            f"Unable to find configuration file. Checked: {cls._config_candidates(path)}",
        )


class BundleAssetRecord(BaseModel):
    bundle_id: str
    asset_type: str
    asset_id: str


class AssetNamePayload(BaseModel):
    asset_id: str
    asset_type: str
    asset_name: str


def chunked(iterable: Iterable[str], size: int) -> Iterable[list[str]]:
    batch: list[str] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch
