#!/usr/bin/env python

"""Unit tests for config module."""

from data_compaction_job.config import clean_option_keys


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