import os
import pytest
from unittest.mock import MagicMock, patch
from config import AppConfig
from scheduler.storage import StorageHandler


@pytest.fixture
def mock_config():
    config = MagicMock(spec=AppConfig)
    config.output_s3_path = "s3://bucket/output"
    return config


@patch("scheduler.storage.s3fs.S3FileSystem")
@patch.dict(os.environ, {"IOMETE_JOB_ID": "job1", "IOMETE_JOB_RUN_ID": "run1"})
def test_upload(mock_s3fs, mock_config):
    mock_fs = MagicMock()
    mock_s3fs.return_value = mock_fs
    
    storage = StorageHandler(mock_config)
    local_file = "/tmp/output_notebook.ipynb"
    
    result = storage.upload(local_file)
    
    expected_path = "s3://bucket/output/job1/run1/output_notebook.ipynb"
    assert result == expected_path
    
    mock_fs.put.assert_called_once_with(local_file, expected_path)
