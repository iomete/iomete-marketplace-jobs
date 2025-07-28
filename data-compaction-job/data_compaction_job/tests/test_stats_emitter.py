#!/usr/bin/env python

"""Unit tests for stats_emitter module."""

from unittest.mock import patch, MagicMock

from data_compaction_job.stats_emitter import _add_orphan_files_metrics


class TestOrphanFilesChunkingLogic:
    """Unit tests for orphan files chunking logic."""
    
    def test_chunking_with_multiple_chunks(self):
        """Test the simplified chunking logic for orphan files metrics with multiple chunks."""
        # Mock the global stats batcher
        mock_batcher = MagicMock()
        mock_batcher.max_files_per_record = 3
        mock_batcher.spark_app_id = "test-app-id"
        
        # Test data
        removed_files = ["file1.txt", "file2.txt", "file3.txt", "file4.txt", "file5.txt"]
        args = [None, "catalog", "db", "table"]
        operation = "REMOVE_ORPHAN_FILES"
        sql = "SELECT * FROM table"
        start_time = 1234567890.0
        end_time = 1234567900.0
        
        # Patch the global batcher
        with patch('data_compaction_job.stats_emitter._stats_batcher', mock_batcher):
            _add_orphan_files_metrics(removed_files, args, operation, sql, start_time, end_time)
        
        # Verify add_metric was called twice (since 5 files with chunk size 3 = 2 chunks)
        assert mock_batcher.add_metric.call_count == 2
        
        # Check first chunk
        first_call = mock_batcher.add_metric.call_args_list[0]
        first_metrics = first_call[1]['metrics']
        assert first_metrics['removed file count'] == '5'
        assert first_metrics['removed files'] == 'file1.txt,\nfile2.txt,\nfile3.txt'
        assert first_metrics['chunk number'] == '1/2'
        
        # Check second chunk
        second_call = mock_batcher.add_metric.call_args_list[1]
        second_metrics = second_call[1]['metrics']
        assert second_metrics['removed file count'] == '5'
        assert second_metrics['removed files'] == 'file4.txt,\nfile5.txt'
        assert second_metrics['chunk number'] == '2/2'

    def test_chunking_with_single_chunk(self):
        """Test the chunking logic when files fit in a single chunk."""
        # Mock the global stats batcher
        mock_batcher = MagicMock()
        mock_batcher.max_files_per_record = 5
        mock_batcher.spark_app_id = "test-app-id"
        
        # Test data - fewer files than chunk size
        removed_files = ["file1.txt", "file2.txt", "file3.txt"]
        args = [None, "catalog", "db", "table"]
        operation = "REMOVE_ORPHAN_FILES"
        sql = "SELECT * FROM table"
        start_time = 1234567890.0
        end_time = 1234567900.0
        
        # Patch the global batcher
        with patch('data_compaction_job.stats_emitter._stats_batcher', mock_batcher):
            _add_orphan_files_metrics(removed_files, args, operation, sql, start_time, end_time)
        
        # Verify add_metric was called once
        assert mock_batcher.add_metric.call_count == 1
        
        # Check metrics
        call_args = mock_batcher.add_metric.call_args_list[0]
        metrics = call_args[1]['metrics']
        assert metrics['removed file count'] == '3'
        assert metrics['removed files'] == 'file1.txt,\nfile2.txt,\nfile3.txt'
        # No chunk number since it's a single chunk
        assert 'chunk number' not in metrics

    def test_chunking_with_empty_files_list(self):
        """Test the chunking logic with an empty files list."""
        # Mock the global stats batcher
        mock_batcher = MagicMock()
        mock_batcher.max_files_per_record = 3
        mock_batcher.spark_app_id = "test-app-id"
        
        # Test data - empty list
        removed_files = []
        args = [None, "catalog", "db", "table"]
        operation = "REMOVE_ORPHAN_FILES"
        sql = "SELECT * FROM table"
        start_time = 1234567890.0
        end_time = 1234567900.0
        
        # Patch the global batcher
        with patch('data_compaction_job.stats_emitter._stats_batcher', mock_batcher):
            _add_orphan_files_metrics(removed_files, args, operation, sql, start_time, end_time)
        
        # Verify add_metric was called once even for empty list
        assert mock_batcher.add_metric.call_count == 1
        
        # Check metrics
        call_args = mock_batcher.add_metric.call_args_list[0]
        metrics = call_args[1]['metrics']
        assert metrics['removed file count'] == '0'
        assert metrics['removed files'] == ''
        # No chunk number since it's a single chunk
        assert 'chunk number' not in metrics