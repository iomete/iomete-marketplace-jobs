import os
import pytest
from unittest.mock import MagicMock, patch
from config import AppConfig
from scheduler.fetcher import InputFetcher

@pytest.fixture
def mock_config():
    config = MagicMock(spec=AppConfig)
    config.input_type = "manual"
    config.input_path = "/tmp/test"
    config.git_branch = "main"
    config.git_token = None
    return config

def test_fetch_manual_no_path(mock_config):
    mock_config.input_path = None
    fetcher = InputFetcher(mock_config)
    path = fetcher.fetch()
    assert os.path.exists(path)
    assert os.path.isdir(path)

@patch("scheduler.fetcher.git.Repo.clone_from")
def test_fetch_git(mock_clone, mock_config):
    mock_config.input_type = "git"
    mock_config.input_path = "https://github.com/test/repo.git"
    
    fetcher = InputFetcher(mock_config)
    path = fetcher.fetch()
    
    assert os.path.exists(path)
    mock_clone.assert_called_once()
    args, kwargs = mock_clone.call_args
    assert args[0] == "https://github.com/test/repo.git"
    assert kwargs["branch"] == "main"

@patch("scheduler.fetcher.s3fs.S3FileSystem")
def test_fetch_s3(mock_s3fs, mock_config):
    mock_config.input_type = "s3"
    mock_config.input_path = "s3://bucket/path"
    
    mock_fs = MagicMock()
    mock_s3fs.return_value = mock_fs
    
    fetcher = InputFetcher(mock_config)
    path = fetcher.fetch()
    
    assert os.path.exists(path)
    mock_fs.get.assert_called_once()
    args, kwargs = mock_fs.get.call_args
    assert args[0] == "s3://bucket/path"
    assert args[1] == path
    assert kwargs["recursive"] == True
