from query_scheduler_job.config import get_config


def test_config_parsing():
    queries = get_config("application.conf")
    assert len(queries) > 0
