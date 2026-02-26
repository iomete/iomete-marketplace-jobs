"""Unit tests for spark-based orphaned asset detection helpers."""

from datetime import datetime

from orphaned_asset_detection import main as detection


def _df(spark, rows, schema):
    return spark.createDataFrame(rows, schema)


def _deleted_users(spark):
    schema = "owner_id string, archive_date timestamp"
    return _df(
        spark,
        [
            ("deleted_user1", datetime(2024, 1, 1)),
            ("deleted_user2", datetime(2024, 1, 2)),
        ],
        schema,
    )


def _deleted_groups(spark):
    schema = "owner_id string, archive_date timestamp"
    return _df(
        spark,
        [
            ("deleted_group1", datetime(2024, 1, 3)),
        ],
        schema,
    )


def test_detect_orphaned_domains_handles_users_and_groups(spark):
    domains = _df(
        spark,
        [
            ("domain1", "Domain One", "domain1", '["deleted_user1", "deleted_group1"]'),
            ("domain2", "Domain Two", "domain2", '["active_user"]'),
        ],
        "asset_id string, asset_name string, domain_id string, owners string",
    )

    result = detection.detect_orphaned_domains(domains, _deleted_users(spark))
    collected = result.collect()

    rows = {(row.asset_id, row.asset_name, row.domain_id, row.owner_type, row.owner_id) for row in collected}
    assert ("domain1", "Domain One", "domain1", "USER", "deleted_user1") in rows
    assert ("domain1", "Domain One", "domain1", "GROUP", "deleted_group1") not in rows
    assert all(row.asset_type == "DOMAIN" for row in collected)
    assert all(row.domain_id == "domain1" for row in collected)


def test_detect_orphaned_bundles_filters_active_owners(spark):
    bundles = _df(
        spark,
        [
            ("1", "Bundle One", "domain1", "deleted_user1", "user"),
            ("2", "Bundle Two", None, "active", "user"),
            ("3", "Bundle Three", "domain2", "deleted_group1", "group"),
            ("4", "Bundle Four", "domain3", "active_group", "group"),
        ],
        "asset_id string, asset_name string, domain_id string, owner_id string, owner_type string",
    )

    result = detection.detect_orphaned_bundles(bundles, _deleted_users(spark), _deleted_groups(spark))
    rows = {(row.asset_id, row.asset_name, row.domain_id, row.owner_type, row.owner_id) for row in result.collect()}

    assert ("1", "Bundle One", "domain1", "USER", "deleted_user1") in rows
    assert ("3", "Bundle Three", "domain2", "GROUP", "deleted_group1") in rows
    assert ("2", "Bundle Two", None, "USER", "active") not in rows
    assert ("4", "Bundle Four", "domain3", "GROUP", "active_group") not in rows


def test_detect_orphaned_policies_supports_identity_types(spark):
    policies = _df(
        spark,
        [
            ("policy1", "Policy One", None, "deleted_user1", "user"),
            ("policy2", "Policy Two", None, "deleted_group1", "group"),
            ("policy3", "Policy Three", None, "active_user", "user"),
        ],
        "asset_id string, asset_name string, domain_id string, identity string, identity_type string",
    )

    result = detection.detect_orphaned_policies(policies, _deleted_users(spark), _deleted_groups(spark))
    rows = {(row.asset_id, row.asset_name, row.domain_id, row.owner_type, row.owner_id) for row in result.collect()}

    assert ("policy1", "Policy One", None, "USER", "deleted_user1") in rows
    assert ("policy2", "Policy Two", None, "GROUP", "deleted_group1") in rows
    assert ("policy3", "Policy Three", None, "USER", "active_user") not in rows


def test_exclude_existing_removes_duplicates(spark):
    schema = detection.RESULT_SCHEMA
    candidates = spark.createDataFrame(
        [
            ("DOMAIN", "domain1", "Domain One", "domain1", "USER", "deleted_user1", datetime(2024, 1, 1)),
            ("BUNDLE", "bundle1", "Bundle One", "domain2", "GROUP", "deleted_group1", datetime(2024, 1, 2)),
        ],
        schema,
    )
    existing = spark.createDataFrame(
        [
            ("DOMAIN", "domain1", "USER", "deleted_user1"),
        ],
        "asset_type string, asset_id string, owner_type string, owner_id string",
    )

    result = detection._exclude_existing(candidates, existing)
    remaining = {(row.asset_type, row.asset_id, row.asset_name, row.domain_id) for row in result.collect()}

    assert remaining == {("BUNDLE", "bundle1", "Bundle One", "domain2")}
