#!/usr/bin/env python

"""Tests for `data_compaction_job.sql_compaction.SqlClient` base URL resolution."""

from unittest.mock import patch

from data_compaction_job.sql_compaction import SqlClient


@patch.dict("os.environ", {}, clear=True)
def test_falls_back_to_in_cluster_address_when_nothing_is_set():
    assert SqlClient().base_url == "http://iom-core.iomete-system.svc.cluster.local"


@patch.dict("os.environ", {"IOMETE_WORKLOAD_CONTROL_PLANE_URL": "http://iom-control-plane-egress:8082"}, clear=True)
def test_prefers_the_workload_control_plane_url_over_the_in_cluster_address():
    assert SqlClient().base_url == "http://iom-control-plane-egress:8082"


@patch.dict(
    "os.environ",
    {
        "SQL_API_ENDPOINT": "http://explicit-override:9000",
        "IOMETE_WORKLOAD_CONTROL_PLANE_URL": "http://iom-control-plane-egress:8082",
    },
    clear=True,
)
def test_an_explicit_sql_api_endpoint_still_wins():
    assert SqlClient().base_url == "http://explicit-override:9000"
