import unittest

import polars as pl

from models import Severity
from rules import (
    find_access_key_variation,
    find_internal_uri_inconsistency,
    find_multiple_managers,
    find_unresolved_external_catalogs,
)

def inventory(rows):
    return pl.DataFrame(rows)


class TestMC001CatalogOwnership(unittest.TestCase):
    def test_detects_same_internal_catalog_in_multiple_environments(self):
        df = inventory(
            [
                {
                    "name": "finance",
                    "catalogType_classification": "internal",
                    "env_name": "a2",
                },
                {
                    "name": "finance",
                    "catalogType_classification": "internal",
                    "env_name": "a3",
                },
            ]
        )

        findings = find_multiple_managers(df)

        self.assertEqual(len(findings), 1)

        finding = findings[0]

        self.assertEqual(finding.rule_id, "MC001")
        self.assertEqual(finding.severity, Severity.HIGH)
        self.assertEqual(finding.catalog, "finance")
        self.assertEqual(
            finding.managers,
            ("a2", "a3"),
        )

    def test_does_not_flag_single_internal_manager(self):
        df = inventory(
            [
                {
                    "name": "finance",
                    "catalogType_classification": "internal",
                    "env_name": "a2",
                }
            ]
        )

        findings = find_multiple_managers(df)

        self.assertEqual(findings, [])

    def test_external_catalog_does_not_create_conflict(self):
        df = inventory(
            [
                {
                    "name": "finance",
                    "catalogType_classification": "internal",
                    "env_name": "a2",
                },
                {
                    "name": "finance",
                    "catalogType_classification": "external",
                    "env_name": "a3",
                },
            ]
        )

        findings = find_multiple_managers(df)

        self.assertEqual(findings, [])

    def test_duplicate_rows_in_same_environment_do_not_create_conflict(self):
        df = inventory(
            [
                {
                    "name": "finance",
                    "catalogType_classification": "internal",
                    "env_name": "a2",
                },
                {
                    "name": "finance",
                    "catalogType_classification": "internal",
                    "env_name": "a2",
                },
            ]
        )

        findings = find_multiple_managers(df)

        self.assertEqual(findings, [])

    def test_system_catalog_is_excluded(self):
        df = inventory(
            [
                {
                    "name": "spark_catalog",
                    "catalogType_classification": "internal",
                    "env_name": "a2",
                },
                {
                    "name": "spark_catalog",
                    "catalogType_classification": "internal",
                    "env_name": "a3",
                },
            ]
        )

        findings = find_multiple_managers(df)

        self.assertEqual(findings, [])

    def test_null_catalog_name_is_ignored(self):
        df = inventory(
            [
                {
                    "name": None,
                    "catalogType_classification": "internal",
                    "env_name": "a2",
                },
                {
                    "name": None,
                    "catalogType_classification": "internal",
                    "env_name": "a3",
                },
            ]
        )

        findings = find_multiple_managers(df)

        self.assertEqual(findings, [])

    def test_missing_required_column_fails_loudly(self):
        df = inventory(
            [
                {
                    "name": "finance",
                    "env_name": "a2",
                }
            ]
        )

        with self.assertRaises(ValueError):
            find_multiple_managers(df)


class TestMC002InternalUriConsistency(unittest.TestCase):
    def _row(
        self,
        *,
        name: str = "finance",
        uri: str | None = "http://catalog-service/internal/catalogs/finance",
        classification: str = "internal",
        catalog_type: str = "iceberg",
        subtype: str = "rest",
        env: str = "a2",
    ):
        return {
            "name": name,
            "catalogType_type": catalog_type,
            "catalogType_subtype": subtype,
            "catalogType_classification": classification,
            "properties_uri": uri,
            "env_name": env,
        }

    def test_valid_internal_rest_catalog_uri_is_not_reported(self):
        df = inventory([self._row()])

        findings = find_internal_uri_inconsistency(df)

        self.assertEqual(findings, [])

    def test_catalog_name_mismatch_is_reported(self):
        df = inventory(
            [
                self._row(
                    name="finance",
                    uri=("http://catalog-service/" "internal/catalogs/hr"),
                )
            ]
        )

        findings = find_internal_uri_inconsistency(df)

        self.assertEqual(len(findings), 1)

        finding = findings[0]

        self.assertEqual(finding.rule_id, "MC002")
        self.assertEqual(finding.severity, Severity.HIGH)
        self.assertEqual(finding.catalog, "finance")
        self.assertIn(
            "Catalog name mismatch",
            finding.details,
        )

    def test_external_path_for_internal_catalog_is_reported(self):
        df = inventory(
            [self._row(uri=("http://catalog-service/" "external/catalogs/finance"))]
        )

        findings = find_internal_uri_inconsistency(df)

        self.assertEqual(len(findings), 1)
        self.assertEqual(
            findings[0].rule_id,
            "MC002",
        )

    def test_missing_uri_is_reported(self):
        df = inventory([self._row(uri=None)])

        findings = find_internal_uri_inconsistency(df)

        self.assertEqual(len(findings), 1)
        self.assertIn(
            "no properties_uri",
            findings[0].details,
        )

    def test_empty_uri_is_reported(self):
        df = inventory([self._row(uri="")])

        findings = find_internal_uri_inconsistency(df)

        self.assertEqual(len(findings), 1)

    def test_malformed_uri_is_reported(self):
        df = inventory([self._row(uri="not-a-valid-uri")])

        findings = find_internal_uri_inconsistency(df)

        self.assertEqual(len(findings), 1)
        self.assertIn(
            "malformed properties_uri",
            findings[0].details,
        )

    def test_trailing_slash_is_accepted(self):
        df = inventory(
            [self._row(uri=("http://catalog-service/" "internal/catalogs/finance/"))]
        )

        findings = find_internal_uri_inconsistency(df)

        self.assertEqual(findings, [])

    def test_https_uri_is_accepted(self):
        df = inventory(
            [self._row(uri=("https://catalog-service/" "internal/catalogs/finance"))]
        )

        findings = find_internal_uri_inconsistency(df)

        self.assertEqual(findings, [])

    def test_external_catalog_is_not_evaluated_by_mc002(self):
        df = inventory(
            [
                self._row(
                    classification="external",
                    uri=("https://a3.example.com/" "catalogs/finance"),
                )
            ]
        )

        findings = find_internal_uri_inconsistency(df)

        self.assertEqual(findings, [])

    def test_non_rest_catalog_is_not_evaluated_by_mc002(self):
        df = inventory(
            [
                self._row(
                    subtype="something_else",
                )
            ]
        )

        findings = find_internal_uri_inconsistency(df)

        self.assertEqual(findings, [])

    def test_non_iceberg_catalog_is_not_evaluated_by_mc002(self):
        df = inventory(
            [
                self._row(
                    catalog_type="other",
                )
            ]
        )

        findings = find_internal_uri_inconsistency(df)

        self.assertEqual(findings, [])

    def test_system_catalog_is_not_evaluated_by_mc002(self):
        df = inventory(
            [
                self._row(
                    name="spark_catalog",
                    uri=("http://catalog-service/" "internal/catalogs/wrong-name"),
                )
            ]
        )

        findings = find_internal_uri_inconsistency(df)

        self.assertEqual(findings, [])

    def test_shared_lakehouse_directory_is_not_an_mc002_finding(self):
        df = inventory(
            [
                {
                    **self._row(
                        name="finance",
                        uri=("http://service-a/" "internal/catalogs/finance"),
                        env="a2",
                    ),
                    "lakehouseDir": "s3://shared-bucket",
                },
                {
                    **self._row(
                        name="hr",
                        uri=("http://service-b/" "internal/catalogs/hr"),
                        env="a3",
                    ),
                    "lakehouseDir": "s3://shared-bucket",
                },
            ]
        )

        findings = find_internal_uri_inconsistency(df)

        self.assertEqual(findings, [])

    def test_missing_required_schema_fails_loudly(self):
        df = inventory(
            [
                {
                    "name": "finance",
                    "env_name": "a2",
                }
            ]
        )

        with self.assertRaises(ValueError):
            find_internal_uri_inconsistency(df)


class TestMC003CredentialConsistency(unittest.TestCase):
    def _row(
        self,
        env,
        key,
        *,
        name="finance",
        storage="s3://finance",
        endpoint="https://storage",
    ):
        return {
            "name": name,
            "lakehouseDir": storage,
            "credentials_endpoint": endpoint,
            "credentials_accessKey": key,
            "env_name": env,
        }

    def test_detects_different_keys_across_environments(self):
        df = inventory(
            [
                self._row("a2", "KEY_A"),
                self._row("a3", "KEY_B"),
            ]
        )

        findings = find_access_key_variation(df)

        self.assertEqual(len(findings), 1)

        finding = findings[0]

        self.assertEqual(finding.rule_id, "MC003")
        self.assertEqual(
            finding.severity,
            Severity.RECOMMENDATION,
        )
        self.assertEqual(
            finding.consumers,
            ("a2", "a3"),
        )

    def test_same_key_is_not_reported(self):
        df = inventory(
            [
                self._row("a2", "KEY_A"),
                self._row("a3", "KEY_A"),
            ]
        )

        findings = find_access_key_variation(df)

        self.assertEqual(findings, [])

    def test_different_catalog_is_not_grouped_together(self):
        df = inventory(
            [
                self._row(
                    "a2",
                    "KEY_A",
                    name="finance",
                ),
                self._row(
                    "a3",
                    "KEY_B",
                    name="hr",
                ),
            ]
        )

        findings = find_access_key_variation(df)

        self.assertEqual(findings, [])

    def test_different_storage_is_not_grouped_together(self):
        df = inventory(
            [
                self._row(
                    "a2",
                    "KEY_A",
                    storage="s3://finance-a",
                ),
                self._row(
                    "a3",
                    "KEY_B",
                    storage="s3://finance-b",
                ),
            ]
        )

        findings = find_access_key_variation(df)

        self.assertEqual(findings, [])

    def test_different_endpoint_is_not_grouped_together(self):
        df = inventory(
            [
                self._row(
                    "a2",
                    "KEY_A",
                    endpoint="https://storage-a",
                ),
                self._row(
                    "a3",
                    "KEY_B",
                    endpoint="https://storage-b",
                ),
            ]
        )

        findings = find_access_key_variation(df)

        self.assertEqual(findings, [])

    def test_single_environment_is_not_reported(self):
        df = inventory(
            [
                self._row("a2", "KEY_A"),
                self._row("a2", "KEY_B"),
            ]
        )

        findings = find_access_key_variation(df)

        self.assertEqual(findings, [])

    def test_null_access_key_is_ignored(self):
        df = inventory(
            [
                self._row("a2", "KEY_A"),
                self._row("a3", None),
            ]
        )

        findings = find_access_key_variation(df)

        self.assertEqual(findings, [])

    def test_system_catalog_is_excluded(self):
        df = inventory(
            [
                self._row(
                    "a2",
                    "KEY_A",
                    name="spark_catalog",
                ),
                self._row(
                    "a3",
                    "KEY_B",
                    name="spark_catalog",
                ),
            ]
        )

        findings = find_access_key_variation(df)

        self.assertEqual(findings, [])

    def test_evidence_groups_environments_without_exposing_key_value(self):
        df = inventory(
            [
                self._row(
                    "a2",
                    "VERY_SECRET_KEY_A",
                ),
                self._row(
                    "a3",
                    "VERY_SECRET_KEY_A",
                ),
                self._row(
                    "a6",
                    "VERY_SECRET_KEY_B",
                ),
            ]
        )

        findings = find_access_key_variation(df)

        self.assertEqual(len(findings), 1)

        evidence = " ".join(findings[0].evidence)

        self.assertIn(
            "Credential 1",
            evidence,
        )
        self.assertIn(
            "Credential 2",
            evidence,
        )

        self.assertIn("a2", evidence)
        self.assertIn("a3", evidence)
        self.assertIn("a6", evidence)

        self.assertNotIn(
            "VERY_SECRET_KEY_A",
            evidence,
        )
        self.assertNotIn(
            "VERY_SECRET_KEY_B",
            evidence,
        )

    def test_missing_required_columns_fails_loudly(self):
        df = inventory(
            [
                {
                    "name": "finance",
                    "env_name": "a2",
                }
            ]
        )

        with self.assertRaises(ValueError):
            find_access_key_variation(df)


class TestMC004ExternalCatalogResolution(unittest.TestCase):

    def _row(
        self,
        *,
        env: str = "a2",
        name: str = "finance",
        classification: str = "external",
        catalog_type: str = "iceberg",
        subtype: str = "rest",
        uri: str | None = "https://catalog.example.com/catalogs/finance",
        storage: str | None = "s3://finance-bucket",
    ):
        return {
            "env_name": env,
            "name": name,
            "catalogType_classification": classification,
            "catalogType_type": catalog_type,
            "catalogType_subtype": subtype,
            "properties_uri": uri,
            "lakehouseDir": storage,
        }

    def test_external_resolves_to_matching_internal(self):
        df = inventory(
            [
                self._row(),
                self._row(
                    env="a3",
                    classification="internal",
                    uri="http://catalog-service/internal/catalogs/finance",
                ),
            ]
        )

        self.assertEqual(
            find_unresolved_external_catalogs(df),
            [],
        )

    def test_external_alias_can_resolve_to_different_target_name(self):
        df = inventory(
            [
                self._row(
                    name="finance_alias",
                    uri="https://catalog.example.com/catalogs/finance",
                ),
                self._row(
                    env="a3",
                    name="finance",
                    classification="internal",
                    uri="http://catalog-service/internal/catalogs/finance",
                ),
            ]
        )

        self.assertEqual(
            find_unresolved_external_catalogs(df),
            [],
        )

    def test_missing_internal_target_is_reported(self):
        df = inventory([self._row()])

        findings = find_unresolved_external_catalogs(df)

        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].rule_id, "MC004")
        self.assertEqual(
            findings[0].severity,
            Severity.HIGH,
        )
        self.assertIn(
            "no internal Iceberg REST catalog",
            findings[0].details,
        )

    def test_target_exists_but_storage_differs_is_reported(self):
        df = inventory(
            [
                self._row(
                    storage="s3://consumer-storage",
                ),
                self._row(
                    env="a3",
                    classification="internal",
                    uri="http://catalog-service/internal/catalogs/finance",
                    storage="s3://manager-storage",
                ),
            ]
        )

        findings = find_unresolved_external_catalogs(df)

        self.assertEqual(len(findings), 1)
        self.assertIn(
            "none use the external catalog's configured storage",
            findings[0].details,
        )

    def test_one_matching_storage_candidate_is_enough(self):
        df = inventory(
            [
                self._row(
                    storage="s3://correct",
                ),
                self._row(
                    env="a3",
                    classification="internal",
                    uri="http://catalog-a/internal/catalogs/finance",
                    storage="s3://wrong",
                ),
                self._row(
                    env="s6",
                    classification="internal",
                    uri="http://catalog-b/internal/catalogs/finance",
                    storage="s3://correct",
                ),
            ]
        )

        self.assertEqual(
            find_unresolved_external_catalogs(df),
            [],
        )

    def test_multiple_matching_managers_are_left_to_mc001(self):
        df = inventory(
            [
                self._row(),
                self._row(
                    env="a3",
                    classification="internal",
                    uri="http://catalog-a/internal/catalogs/finance",
                ),
                self._row(
                    env="s6",
                    classification="internal",
                    uri="http://catalog-b/internal/catalogs/finance",
                ),
            ]
        )

        self.assertEqual(
            find_unresolved_external_catalogs(df),
            [],
        )

        mc001 = find_multiple_managers(df)

        self.assertEqual(len(mc001), 1)

    def test_missing_uri_is_reported(self):
        df = inventory(
            [
                self._row(uri=None),
            ]
        )

        findings = find_unresolved_external_catalogs(df)

        self.assertEqual(len(findings), 1)
        self.assertIn(
            "no properties_uri",
            findings[0].details,
        )

    def test_empty_uri_is_reported(self):
        df = inventory(
            [
                self._row(uri=""),
            ]
        )

        findings = find_unresolved_external_catalogs(df)

        self.assertEqual(len(findings), 1)

    def test_malformed_uri_is_reported(self):
        df = inventory(
            [
                self._row(uri="not-a-uri"),
            ]
        )

        findings = find_unresolved_external_catalogs(df)

        self.assertEqual(len(findings), 1)
        self.assertIn(
            "malformed properties_uri",
            findings[0].details,
        )

    def test_uri_without_catalogs_path_is_reported(self):
        df = inventory(
            [
                self._row(
                    uri="https://catalog.example.com/finance",
                ),
            ]
        )

        findings = find_unresolved_external_catalogs(df)

        self.assertEqual(len(findings), 1)
        self.assertIn(
            "/catalogs/<catalog-name>",
            findings[0].details,
        )

    def test_trailing_slash_is_accepted(self):
        df = inventory(
            [
                self._row(
                    uri="https://catalog.example.com/catalogs/finance/",
                ),
                self._row(
                    env="a3",
                    classification="internal",
                    uri="http://catalog-service/internal/catalogs/finance",
                ),
            ]
        )

        self.assertEqual(
            find_unresolved_external_catalogs(df),
            [],
        )

    def test_url_encoded_target_name_is_decoded(self):
        df = inventory(
            [
                self._row(
                    name="local_alias",
                    uri=("https://catalog.example.com/" "catalogs/finance%20data"),
                ),
                self._row(
                    env="a3",
                    name="finance data",
                    classification="internal",
                    uri=("http://catalog-service/" "internal/catalogs/finance%20data"),
                ),
            ]
        )

        self.assertEqual(
            find_unresolved_external_catalogs(df),
            [],
        )

    def test_jdbc_external_catalog_is_ignored(self):
        df = inventory(
            [
                self._row(
                    catalog_type="jdbc",
                    subtype="postgres",
                ),
            ]
        )

        self.assertEqual(
            find_unresolved_external_catalogs(df),
            [],
        )

    def test_non_rest_external_catalog_is_ignored(self):
        df = inventory(
            [
                self._row(subtype="hadoop"),
            ]
        )

        self.assertEqual(
            find_unresolved_external_catalogs(df),
            [],
        )

    def test_internal_catalog_is_not_evaluated_as_external(self):
        df = inventory(
            [
                self._row(
                    classification="internal",
                    uri=("http://catalog-service/" "internal/catalogs/finance"),
                ),
            ]
        )

        self.assertEqual(
            find_unresolved_external_catalogs(df),
            [],
        )

    def test_system_catalog_is_ignored(self):
        df = inventory(
            [
                self._row(name="spark_catalog"),
            ]
        )

        self.assertEqual(
            find_unresolved_external_catalogs(df),
            [],
        )

    def test_missing_storage_is_reported_when_target_exists(self):
        df = inventory(
            [
                self._row(storage=None),
                self._row(
                    env="a3",
                    classification="internal",
                    uri="http://catalog-service/internal/catalogs/finance",
                ),
            ]
        )

        findings = find_unresolved_external_catalogs(df)

        self.assertEqual(len(findings), 1)

    def test_missing_required_schema_fails_loudly(self):
        df = inventory(
            [
                {
                    "name": "finance",
                    "env_name": "a2",
                }
            ]
        )

        with self.assertRaises(ValueError):
            find_unresolved_external_catalogs(df)


if __name__ == "__main__":
    unittest.main()
