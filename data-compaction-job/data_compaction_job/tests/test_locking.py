from unittest.mock import patch
from datetime import datetime, timedelta, timezone
import uuid
import os

from data_compaction_job.config import ApplicationConfig, LockConfig
from data_compaction_job.locking import TablePropertyLock
from data_compaction_job.tests._spark_session import get_spark_session


def make_config(lock_enabled=True, ttl_seconds=3600):
    cfg = ApplicationConfig()
    cfg.catalog = "spark_catalog"
    cfg.lock = LockConfig(enabled=lock_enabled, ttl_seconds=ttl_seconds)
    return cfg


class TestTableSetup:
    """Base class for tests that need Iceberg tables."""
    
    @classmethod
    def setup_class(cls):
        """Set up Spark session and test database."""
        cls.spark = get_spark_session()
        cls.test_db = "test_locking"
        cls.test_table = "lock_test_table"
        
        # Create test database
        cls.spark.sql(f"CREATE DATABASE IF NOT EXISTS {cls.test_db}")
    
    @classmethod
    def teardown_class(cls):
        """Clean up test database."""
        cls.spark.sql(f"DROP DATABASE IF EXISTS {cls.test_db} CASCADE")
        cls.spark.stop()
    
    def setup_method(self):
        """Set up test table for each test."""
        # Drop table if exists
        self.spark.sql(f"DROP TABLE IF EXISTS {self.test_db}.{self.test_table}")
        
        # Create fresh Iceberg table
        self.spark.sql(f"""
            CREATE TABLE {self.test_db}.{self.test_table} (
                id BIGINT,
                name STRING,
                value INT
            )
            USING iceberg
        """)
    
    def teardown_method(self):
        """Clean up test table after each test."""
        self.spark.sql(f"DROP TABLE IF EXISTS {self.test_db}.{self.test_table}")
    
    def get_table_properties(self):
        """Get current table properties as a dictionary."""
        props = self.spark.sql(f"SHOW TBLPROPERTIES {self.test_db}.{self.test_table}").collect()
        return {row.key: row.value for row in props}
    
    def has_lock_property(self):
        """Check if table has the lock property."""
        props = self.get_table_properties()
        return TablePropertyLock.LOCK_KEY in props
    
    def get_lock_value(self):
        """Get the current lock value if it exists."""
        props = self.get_table_properties()
        return props.get(TablePropertyLock.LOCK_KEY)


class TestTablePropertyLock(TestTableSetup):
    """Integration tests for TablePropertyLock functionality."""
    
    def test_successful_lock_acquisition_and_release(self):
        """Test successful lock acquisition followed by release."""
        config = make_config(lock_enabled=True)
        lock = TablePropertyLock(self.spark, config, owner_id="owner", nonce="nonce")
        
        # Initially no lock should exist
        assert not self.has_lock_property()
        
        # Acquire lock
        acquired = lock.acquire("spark_catalog", self.test_db, self.test_table)
        assert acquired is True
        
        # Verify lock exists
        assert self.has_lock_property()
        lock_value = self.get_lock_value()
        assert "ownerId=owner" in lock_value
        assert "nonce=nonce" in lock_value
        assert "expiresAt=" in lock_value
        
        # Release lock
        lock.release("spark_catalog", self.test_db, self.test_table)
        
        # Verify lock is removed
        assert not self.has_lock_property()
    
    def test_lock_held_by_other_owner_fails_immediately(self):
        """Test that lock acquisition fails immediately when held by another owner."""
        config = make_config(lock_enabled=True)
        
        # Set up existing lock by another owner
        future = (datetime.now(timezone.utc) + timedelta(hours=1)).replace(microsecond=0).isoformat()
        lock_value = f"ownerId=other;nonce=other_nonce;expiresAt={future};version=1"
        self.spark.sql(f"""
            ALTER TABLE {self.test_db}.{self.test_table} 
            SET TBLPROPERTIES ('{TablePropertyLock.LOCK_KEY}' = '{lock_value}')
        """)
        
        # Try to acquire lock with different owner
        lock = TablePropertyLock(self.spark, config, owner_id="owner", nonce="nonce")
        acquired = lock.acquire("spark_catalog", self.test_db, self.test_table)
        assert acquired is False
        
        # Original lock should still be there
        assert self.get_lock_value() == lock_value
    
    def test_malformed_lock_value_allows_takeover(self):
        """Test that malformed lock values are treated as expired and allow takeover."""
        config = make_config(lock_enabled=True)
        
        # Set malformed lock value
        self.spark.sql(f"""
            ALTER TABLE {self.test_db}.{self.test_table} 
            SET TBLPROPERTIES ('{TablePropertyLock.LOCK_KEY}' = 'garbage')
        """)
        
        # Should be able to acquire lock
        lock = TablePropertyLock(self.spark, config, owner_id="owner", nonce="nonce")
        acquired = lock.acquire("spark_catalog", self.test_db, self.test_table)
        assert acquired is True
        
        # Verify new lock value
        lock_value = self.get_lock_value()
        assert "ownerId=owner" in lock_value
        assert "nonce=nonce" in lock_value
    
    def test_expired_lock_allows_takeover(self):
        """Test that expired locks allow takeover by new owner."""
        config = make_config(lock_enabled=True)
        
        # Set expired lock
        past = (datetime.now(timezone.utc) - timedelta(hours=1)).replace(microsecond=0).isoformat()
        expired_lock = f"ownerId=other;nonce=other_nonce;expiresAt={past};version=1"
        self.spark.sql(f"""
            ALTER TABLE {self.test_db}.{self.test_table} 
            SET TBLPROPERTIES ('{TablePropertyLock.LOCK_KEY}' = '{expired_lock}')
        """)
        
        # Should be able to take over expired lock
        lock = TablePropertyLock(self.spark, config, owner_id="owner", nonce="nonce")
        acquired = lock.acquire("spark_catalog", self.test_db, self.test_table)
        assert acquired is True
        
        # Verify new lock value
        lock_value = self.get_lock_value()
        assert "ownerId=owner" in lock_value
        assert "nonce=nonce" in lock_value
    
    def test_lock_without_expiry_allows_takeover(self):
        """Test that locks without expiry timestamp allow takeover."""
        config = make_config(lock_enabled=True)
        
        # Set lock without expiry
        no_expiry_lock = "ownerId=other;nonce=other_nonce;version=1"
        self.spark.sql(f"""
            ALTER TABLE {self.test_db}.{self.test_table} 
            SET TBLPROPERTIES ('{TablePropertyLock.LOCK_KEY}' = '{no_expiry_lock}')
        """)
        
        # Should be able to take over lock without expiry
        lock = TablePropertyLock(self.spark, config, owner_id="owner", nonce="nonce")
        acquired = lock.acquire("spark_catalog", self.test_db, self.test_table)
        assert acquired is True
        
        # Verify new lock value
        lock_value = self.get_lock_value()
        assert "ownerId=owner" in lock_value
        assert "nonce=nonce" in lock_value
    
    def test_release_only_releases_own_lock(self):
        """Test that release only works for locks owned by the same owner/nonce."""
        config = make_config(lock_enabled=True)
        
        # Set lock by different owner
        other_lock = "ownerId=other;nonce=other_nonce;expiresAt=2025-01-01T00:00:00;version=1"
        self.spark.sql(f"""
            ALTER TABLE {self.test_db}.{self.test_table} 
            SET TBLPROPERTIES ('{TablePropertyLock.LOCK_KEY}' = '{other_lock}')
        """)
        
        # Try to release with different owner
        lock = TablePropertyLock(self.spark, config, owner_id="owner", nonce="nonce")
        lock.release("spark_catalog", self.test_db, self.test_table)
        
        # Other owner's lock should still be there
        assert self.get_lock_value() == other_lock
    
    def test_successful_lock_lifecycle(self):
        """Test complete lock lifecycle: acquire, verify, release."""
        config = make_config(lock_enabled=True, ttl_seconds=300)
        lock = TablePropertyLock(self.spark, config, owner_id="test_owner", nonce="test_nonce")
        
        # Acquire lock
        acquired = lock.acquire("spark_catalog", self.test_db, self.test_table)
        assert acquired is True
        
        # Verify lock details
        lock_value = self.get_lock_value()
        assert "ownerId=test_owner" in lock_value
        assert "nonce=test_nonce" in lock_value
        assert "version=1" in lock_value
        
        # Parse expiry time and verify it's approximately 300 seconds in the future
        parts = dict(part.split('=', 1) for part in lock_value.split(';') if '=' in part)
        expires_at = datetime.fromisoformat(parts['expiresAt'])
        now = datetime.now(timezone.utc)
        time_diff = (expires_at - now).total_seconds()
        assert 295 <= time_diff <= 305  # Allow 5 second tolerance
        
        # Release lock
        lock.release("spark_catalog", self.test_db, self.test_table)
        assert not self.has_lock_property()


class TestTablePropertyLockConfiguration(TestTableSetup):
    """Integration tests for TablePropertyLock configuration and initialization."""
    
    def test_default_owner_id_from_environment(self):
        """Test that owner_id defaults to environment variables."""
        with patch.dict(os.environ, {'POD_NAME': 'test-pod'}):
            config = make_config()
            lock = TablePropertyLock(self.spark, config)
            assert lock.owner_id == 'test-pod'
    
    def test_default_owner_id_fallback_to_hostname(self):
        """Test that owner_id falls back to HOSTNAME if POD_NAME not available."""
        with patch.dict(os.environ, {'HOSTNAME': 'test-host'}, clear=True):
            config = make_config()
            lock = TablePropertyLock(self.spark, config)
            assert lock.owner_id == 'test-host'
    
    def test_default_owner_id_fallback_to_uuid(self):
        """Test that owner_id falls back to UUID if no environment variables available."""
        with patch.dict(os.environ, {}, clear=True):
            config = make_config()
            lock = TablePropertyLock(self.spark, config)
            # Should be a valid UUID
            try:
                uuid.UUID(lock.owner_id)
            except ValueError:
                assert False, "owner_id should be a valid UUID when no env vars available"
    
    def test_custom_owner_id_overrides_environment(self):
        """Test that explicitly provided owner_id overrides environment variables."""
        with patch.dict(os.environ, {'POD_NAME': 'test-pod'}):
            config = make_config()
            lock = TablePropertyLock(self.spark, config, owner_id="custom-owner")
            assert lock.owner_id == "custom-owner"
    
    def test_default_nonce_is_uuid(self):
        """Test that default nonce is a valid UUID."""
        config = make_config()
        lock = TablePropertyLock(self.spark, config, owner_id="owner")
        # Should be a valid UUID
        try:
            uuid.UUID(lock.nonce)
        except ValueError:
            assert False, "Default nonce should be a valid UUID"
    
    def test_custom_nonce_overrides_default(self):
        """Test that explicitly provided nonce overrides default UUID."""
        config = make_config()
        lock = TablePropertyLock(self.spark, config, owner_id="owner", nonce="custom-nonce")
        assert lock.nonce == "custom-nonce"
    
    def test_lock_key_constant(self):
        """Test that the lock key constant is correctly defined."""
        assert TablePropertyLock.LOCK_KEY == "iomete.compaction.lock"
    
    def test_ttl_seconds_from_config(self):
        """Test that TTL seconds are read from configuration."""
        config = make_config(ttl_seconds=7200)
        lock = TablePropertyLock(self.spark, config, owner_id="owner", nonce="nonce")
        
        # Acquire lock and verify TTL is used
        acquired = lock.acquire("spark_catalog", self.test_db, self.test_table)
        assert acquired is True
        
        # Parse lock value and verify TTL
        lock_value = self.get_lock_value()
        parts = dict(part.split('=', 1) for part in lock_value.split(';') if '=' in part)
        expires_at = datetime.fromisoformat(parts['expiresAt'])
        now = datetime.now(timezone.utc)
        time_diff = (expires_at - now).total_seconds()
        
        # Should be approximately 7200 seconds (2 hours) in the future
        assert 7195 <= time_diff <= 7205  # Allow 5 second tolerance
    
    def test_default_ttl_when_no_lock_config(self):
        """Test that default TTL is used when lock config is missing."""
        config = ApplicationConfig()
        config.catalog = "spark_catalog"
        config.lock = None  # No lock configuration
        
        lock = TablePropertyLock(self.spark, config, owner_id="owner", nonce="nonce")
        
        # Should use default TTL of 172800 seconds (48 hours)
        acquired = lock.acquire("spark_catalog", self.test_db, self.test_table)
        assert acquired is True
        
        # Parse lock value and verify default TTL
        lock_value = self.get_lock_value()
        parts = dict(part.split('=', 1) for part in lock_value.split(';') if '=' in part)
        expires_at = datetime.fromisoformat(parts['expiresAt'])
        now = datetime.now(timezone.utc)
        time_diff = (expires_at - now).total_seconds()
        
        # Should be approximately 172800 seconds (48 hours) in the future
        assert 172795 <= time_diff <= 172805  # Allow 5 second tolerance


class TestTablePropertyLockEdgeCases(TestTableSetup):
    """Integration tests for edge cases and error scenarios in table locking."""
    
    def test_acquire_with_empty_lock_property_succeeds(self):
        """Test that acquire succeeds when lock property exists but is empty."""
        config = make_config(lock_enabled=True)
        
        # Set empty lock value
        self.spark.sql(f"""
            ALTER TABLE {self.test_db}.{self.test_table} 
            SET TBLPROPERTIES ('{TablePropertyLock.LOCK_KEY}' = '')
        """)
        
        # Should be able to acquire lock
        lock = TablePropertyLock(self.spark, config, owner_id="owner", nonce="nonce")
        acquired = lock.acquire("spark_catalog", self.test_db, self.test_table)
        assert acquired is True
        
        # Verify new lock value
        lock_value = self.get_lock_value()
        assert "ownerId=owner" in lock_value
        assert "nonce=nonce" in lock_value
    
    def test_acquire_with_partial_lock_data_succeeds(self):
        """Test that acquire succeeds when lock data is partially valid."""
        config = make_config(lock_enabled=True)
        
        # Set partially valid lock
        self.spark.sql(f"""
            ALTER TABLE {self.test_db}.{self.test_table} 
            SET TBLPROPERTIES ('{TablePropertyLock.LOCK_KEY}' = 'ownerId=other;version=1')
        """)
        
        # Should be able to acquire lock (missing expiry treated as expired)
        lock = TablePropertyLock(self.spark, config, owner_id="owner", nonce="nonce")
        acquired = lock.acquire("spark_catalog", self.test_db, self.test_table)
        assert acquired is True
        
        # Verify new lock value
        lock_value = self.get_lock_value()
        assert "ownerId=owner" in lock_value
        assert "nonce=nonce" in lock_value
    
    def test_acquire_handles_invalid_datetime_format(self):
        """Test that acquire handles invalid datetime formats gracefully."""
        config = make_config(lock_enabled=True)
        
        # Set lock with invalid datetime
        self.spark.sql(f"""
            ALTER TABLE {self.test_db}.{self.test_table} 
            SET TBLPROPERTIES ('{TablePropertyLock.LOCK_KEY}' = 'ownerId=other;expiresAt=invalid-date;version=1')
        """)
        
        # Should be able to acquire lock (invalid datetime treated as expired)
        lock = TablePropertyLock(self.spark, config, owner_id="owner", nonce="nonce")
        acquired = lock.acquire("spark_catalog", self.test_db, self.test_table)
        assert acquired is True
        
        # Verify new lock value
        lock_value = self.get_lock_value()
        assert "ownerId=owner" in lock_value
        assert "nonce=nonce" in lock_value
    
    def test_acquire_with_very_long_ttl(self):
        """Test acquire with very long TTL values."""
        config = make_config(ttl_seconds=365*24*60*60)  # 1 year
        lock = TablePropertyLock(self.spark, config, owner_id="owner", nonce="nonce")
        
        acquired = lock.acquire("spark_catalog", self.test_db, self.test_table)
        assert acquired is True
        
        # Verify TTL is approximately 1 year
        lock_value = self.get_lock_value()
        parts = dict(part.split('=', 1) for part in lock_value.split(';') if '=' in part)
        expires_at = datetime.fromisoformat(parts['expiresAt'])
        now = datetime.now(timezone.utc)
        time_diff = (expires_at - now).total_seconds()
        
        # Should be approximately 1 year in the future
        expected_seconds = 365*24*60*60
        assert expected_seconds - 10 <= time_diff <= expected_seconds + 10
    
    def test_acquire_with_zero_ttl(self):
        """Test acquire with zero TTL (immediately expired)."""
        config = make_config(ttl_seconds=0)
        lock = TablePropertyLock(self.spark, config, owner_id="owner", nonce="nonce")
        
        acquired = lock.acquire("spark_catalog", self.test_db, self.test_table)
        assert acquired is True
        
        # Lock should exist but be immediately expired
        lock_value = self.get_lock_value()
        parts = dict(part.split('=', 1) for part in lock_value.split(';') if '=' in part)
        expires_at = datetime.fromisoformat(parts['expiresAt'])
        now = datetime.now(timezone.utc)
        
        # Should be at or before current time
        assert expires_at <= now + timedelta(seconds=1)  # Allow 1 second tolerance
    
    def test_release_with_no_existing_lock(self):
        """Test that release works gracefully when no lock exists."""
        config = make_config(lock_enabled=True)
        lock = TablePropertyLock(self.spark, config, owner_id="owner", nonce="nonce")
        
        # Should not raise any exception
        lock.release("spark_catalog", self.test_db, self.test_table)
        
        # Still no lock should exist
        assert not self.has_lock_property()
