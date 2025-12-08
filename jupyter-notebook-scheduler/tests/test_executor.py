import os
import pytest
from unittest.mock import MagicMock, patch
from config import AppConfig
from scheduler.executor import NotebookExecutor

@pytest.fixture
def mock_config():
    config = MagicMock(spec=AppConfig)
    config.main_notebook_file = "notebook.ipynb"
    config.notebook_params = {"p1": "v1"}
    return config

@pytest.fixture
def working_dir(tmp_path):
    # Create a dummy notebook file
    nb_path = tmp_path / "notebook.ipynb"
    nb_path.write_text("{}")
    return str(tmp_path)

@patch("scheduler.executor.pm.execute_notebook")
def test_execute_success(mock_pm_execute, mock_config, working_dir):
    executor = NotebookExecutor(mock_config, working_dir)
    output_path = executor.execute()
    
    assert "output_notebook.ipynb" in output_path
    assert os.path.dirname(output_path) == working_dir
    
    mock_pm_execute.assert_called_once()
    args, kwargs = mock_pm_execute.call_args
    assert args[0] == os.path.join(working_dir, "notebook.ipynb")
    assert args[1] == output_path
    assert kwargs["parameters"] == {"p1": "v1"}
    assert kwargs["cwd"] == working_dir

def test_get_output_path(mock_config, working_dir):
    executor = NotebookExecutor(mock_config, working_dir)
    output_path = executor.get_output_path()
    assert output_path == os.path.join(working_dir, "output_notebook.ipynb")

def test_execute_missing_file(mock_config, tmp_path):
    # Empty dir, no notebook
    executor = NotebookExecutor(mock_config, str(tmp_path))
    with pytest.raises(FileNotFoundError):
        executor.execute()
