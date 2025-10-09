#!/usr/bin/env python

"""Unit tests for config module."""

import tempfile
import os
from data_compaction_job.config import (
    clean_option_keys,
    get_config,
    ExpireSnapshotConfig,
    RemoveOrphanFilesConfig,
    RewriteDataFilesConfig,
    RewriteManifestsConfig
)


class TestCleanOptionKeys:
    """Unit tests for clean_option_keys function."""
    
    def test_clean_single_quotes(self):
        """Test removing single quotes from keys."""
        input_dict = {"'min-input-files'": 2, "'target-file-size-bytes'": 1024}
        expected = {"min-input-files": 2, "target-file-size-bytes": 1024}
        result = clean_option_keys(input_dict)
        assert result == expected
    
    def test_clean_double_quotes(self):
        """Test removing double quotes from keys."""
        input_dict = {'"min-input-files"': 2, '"target-file-size-bytes"': 1024}
        expected = {"min-input-files": 2, "target-file-size-bytes": 1024}
        result = clean_option_keys(input_dict)
        assert result == expected
    
    def test_clean_mixed_quotes(self):
        """Test handling mixed quote types in keys."""
        input_dict = {"'min-input-files'": 2, '"target-file-size-bytes"': 1024, "normal-key": 512}
        expected = {"min-input-files": 2, "target-file-size-bytes": 1024, "normal-key": 512}
        result = clean_option_keys(input_dict)
        assert result == expected
    
    def test_no_quotes_unchanged(self):
        """Test that keys without quotes remain unchanged."""
        input_dict = {"min-input-files": 2, "target-file-size-bytes": 1024}
        expected = {"min-input-files": 2, "target-file-size-bytes": 1024}
        result = clean_option_keys(input_dict)
        assert result == expected
    
    def test_empty_dict(self):
        """Test handling empty dictionary."""
        input_dict = {}
        expected = {}
        result = clean_option_keys(input_dict)
        assert result == expected
    
    def test_none_input(self):
        """Test handling None input."""
        input_dict = None
        result = clean_option_keys(input_dict)
        assert result is None
    
    def test_non_string_keys(self):
        """Test handling non-string keys."""
        input_dict = {123: "value", ("tuple", "key"): "another_value"}
        expected = {123: "value", ("tuple", "key"): "another_value"}
        result = clean_option_keys(input_dict)
        assert result == expected
    
    def test_preserve_original_dict(self):
        """Test that original dictionary is not modified."""
        input_dict = {"'quoted-key'": "value"}
        original_keys = list(input_dict.keys())
        clean_option_keys(input_dict)
        assert list(input_dict.keys()) == original_keys


class TestOperationEnabledFlag:
    """Unit tests for operation enabled flag functionality."""

    def test_default_enabled_values(self):
        """Test that all operations are enabled by default."""
        expire_config = ExpireSnapshotConfig()
        assert expire_config.enabled is True

        remove_orphan_config = RemoveOrphanFilesConfig()
        assert remove_orphan_config.enabled is True

        rewrite_data_config = RewriteDataFilesConfig()
        assert rewrite_data_config.enabled is True

        rewrite_manifest_config = RewriteManifestsConfig()
        assert rewrite_manifest_config.enabled is True

    def test_parse_config_with_enabled_false(self):
        """Test parsing config with enabled set to false for operations."""
        config_content = """
        {
            catalog: "spark_catalog"
            expire_snapshot: {
                enabled: false
                retain_last: 5
            }
            rewrite_data_files: {
                enabled: false
                options: {
                    "min-input-files": 3
                }
            }
            rewrite_manifests: {
                enabled: true
            }
            remove_orphan_files: {
                enabled: false
                older_than_days: 2
            }
        }
        """

        with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as f:
            f.write(config_content)
            config_file = f.name

        try:
            config = get_config(config_file)

            assert config.expire_snapshot.enabled is False
            assert config.expire_snapshot.retain_last == 5

            assert config.rewrite_data_files.enabled is False
            assert config.rewrite_data_files.options == {"min-input-files": 3}

            assert config.rewrite_manifests.enabled is True

            assert config.remove_orphan_files.enabled is False
            assert config.remove_orphan_files.older_than_days == 2
        finally:
            os.unlink(config_file)

    def test_parse_config_without_enabled_field(self):
        """Test that enabled defaults to True when not specified in config."""
        config_content = """
        {
            catalog: "spark_catalog"
            expire_snapshot: {
                retain_last: 3
            }
            rewrite_data_files: {
                options: {
                    "min-input-files": 2
                }
            }
        }
        """

        with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as f:
            f.write(config_content)
            config_file = f.name

        try:
            config = get_config(config_file)

            # All operations should be enabled by default
            assert config.expire_snapshot.enabled is True
            assert config.rewrite_data_files.enabled is True
            assert config.rewrite_manifests.enabled is True
            assert config.remove_orphan_files.enabled is True
        finally:
            os.unlink(config_file)

    def test_parse_config_mixed_enabled_states(self):
        """Test parsing config with mixed enabled states."""
        config_content = """
        {
            catalog: "spark_catalog"
            expire_snapshot: {
                enabled: true
                retain_last: 2
            }
            rewrite_data_files: {
                enabled: false
            }
            rewrite_manifests: {
                enabled: false
                use_caching: true
            }
            remove_orphan_files: {
                enabled: true
                older_than_days: 3
            }
        }
        """

        with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as f:
            f.write(config_content)
            config_file = f.name

        try:
            config = get_config(config_file)

            assert config.expire_snapshot.enabled is True
            assert config.rewrite_data_files.enabled is False
            assert config.rewrite_manifests.enabled is False
            assert config.remove_orphan_files.enabled is True
        finally:
            os.unlink(config_file)

    def test_parse_config_with_table_overrides_enabled(self):
        """Test parsing config with table-level enabled overrides."""
        config_content = """
        {
            catalog: "spark_catalog"
            expire_snapshot: {
                enabled: true
                retain_last: 1
            }
            table_overrides: {
                db1.table1: {
                    expire_snapshot: {
                        enabled: false
                        retain_last: 10
                    }
                    rewrite_data_files: {
                        enabled: false
                    }
                }
                table2: {
                    remove_orphan_files: {
                        enabled: false
                    }
                }
            }
        }
        """

        with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as f:
            f.write(config_content)
            config_file = f.name

        try:
            config = get_config(config_file)

            # Global config should be enabled
            assert config.expire_snapshot.enabled is True

            # Table overrides should be present
            assert config.table_overrides is not None
            assert "db1.table1" in config.table_overrides
            assert config.table_overrides["db1.table1"]["expire_snapshot"]["enabled"] is False
            assert config.table_overrides["db1.table1"]["rewrite_data_files"]["enabled"] is False
            assert config.table_overrides["table2"]["remove_orphan_files"]["enabled"] is False
        finally:
            os.unlink(config_file)