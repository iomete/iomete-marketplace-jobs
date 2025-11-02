import logging
import os
import uuid
from datetime import datetime, timedelta, timezone

from data_compaction_job.config import ApplicationConfig


logger = logging.getLogger(__name__)


class TablePropertyLock:
    """Iceberg table property-based lock with TTL and no heartbeat.

    - Fixed key: iomete.compaction.lock
    - Single acquire with up to 3 attempts on unexpected errors
    - Skip immediately when lock is clearly held by someone else
    - Release is best-effort and idempotent
    """

    LOCK_KEY = "iomete.compaction.lock"

    def __init__(self, spark, config: ApplicationConfig, owner_id: str | None = None, nonce: str | None = None):
        self.spark = spark
        self.config = config
        self.owner_id = owner_id or os.getenv("POD_NAME") or os.getenv("HOSTNAME") or str(uuid.uuid4())
        self.nonce = nonce or str(uuid.uuid4())

    def acquire(self, catalog: str, database: str, table_name: str) -> bool:
        attempts = 0
        max_attempts = 3  # initial + 2 retries for unexpected failures
        while attempts < max_attempts:
            attempts += 1
            try:
                props = {row.key: row.value for row in self.spark.sql(
                    f"SHOW TBLPROPERTIES {catalog}.{database}.{table_name}").collect()}
            except Exception as e:
                if attempts < max_attempts:
                    logger.warning(f"[{database}.{table_name}] Read table properties failed (attempt {attempts}/{max_attempts}), will retry: {e}")
                    continue
                logger.error(f"[{database}.{table_name}] Failed to read table properties for lock: {e}")
                return False

            now = datetime.now(timezone.utc)
            ttl_seconds = int(self.config.lock.ttl_seconds) if self.config.lock else 172800

            current = props.get(self.LOCK_KEY)
            if current:
                try:
                    parts = dict(part.split('=', 1) for part in current.split(';') if '=' in part)
                    expires_at = datetime.fromisoformat(parts.get('expiresAt')) if parts.get('expiresAt') else None
                    if expires_at and expires_at > now:
                        logger.info(f"[{database}.{table_name}] Lock held until {expires_at.isoformat()}; skipping.")
                        return False  # do not retry when clearly held
                except Exception:
                    # Malformed lock value: proceed to attempt takeover
                    pass

            expires_at = (now + timedelta(seconds=ttl_seconds)).replace(microsecond=0)
            value = f"ownerId={self.owner_id};nonce={self.nonce};expiresAt={expires_at.isoformat()};version=1"
            try:
                self.spark.sql(
                    f"ALTER TABLE {catalog}.{database}.{table_name} SET TBLPROPERTIES ('{self.LOCK_KEY}' = '{value}')").collect()
                logger.info(f"[{database}.{table_name}] Acquired compaction lock until {expires_at.isoformat()} (owner={self.owner_id})")
                return True
            except Exception as e:
                if attempts < max_attempts:
                    logger.warning(f"[{database}.{table_name}] Lock set failed (attempt {attempts}/{max_attempts}), will retry: {e}")
                    continue
                logger.info(f"[{database}.{table_name}] Failed to acquire lock after {max_attempts} attempts: {e}")
                return False

    def release(self, catalog: str, database: str, table_name: str) -> None:
        try:
            props = {row.key: row.value for row in self.spark.sql(
                f"SHOW TBLPROPERTIES {catalog}.{database}.{table_name}").collect()}
        except Exception as e:
            logger.warning(f"[{database}.{table_name}] Could not read properties for lock release: {e}")
            return

        current = props.get(self.LOCK_KEY)
        if current:
            try:
                parts = dict(part.split('=', 1) for part in current.split(';') if '=' in part)
                if parts.get('ownerId') == self.owner_id and parts.get('nonce') == self.nonce:
                    try:
                        self.spark.sql(
                            f"ALTER TABLE {catalog}.{database}.{table_name} UNSET TBLPROPERTIES ('{self.LOCK_KEY}')").collect()
                        logger.info(f"[{database}.{table_name}] Released compaction lock (owner={self.owner_id})")
                    except Exception as e:
                        logger.warning(f"[{database}.{table_name}] Failed to release lock: {e}")
                else:
                    logger.info(f"[{database}.{table_name}] Lock ownership changed; skipping release")
            except Exception:
                # Malformed current value, attempt best-effort unset
                try:
                    self.spark.sql(
                        f"ALTER TABLE {catalog}.{database}.{table_name} UNSET TBLPROPERTIES ('{self.LOCK_KEY}')").collect()
                except Exception:
                    pass
        # else: nothing to release


