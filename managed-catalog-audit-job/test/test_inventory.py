import unittest
from unittest.mock import patch

import polars as pl

from config import Environment
from inventory import (
    catalogs_to_dataframe,
    fetch_catalog_inventory,
)


class TestCatalogFlattening(unittest.TestCase):
    def test_flattens_nested_dictionary(self):
        data = {
            "items": [
                {
                    "name": "finance",
                    "credentials": {
                        "endpoint": "https://storage",
                        "accessKey": "KEY",
                    },
                }
            ]
        }

        df = catalogs_to_dataframe(data)

        self.assertEqual(
            df["name"][0],
            "finance",
        )

        self.assertEqual(
            df["credentials_endpoint"][0],
            "https://storage",
        )

        self.assertEqual(
            df["credentials_accessKey"][0],
            "KEY",
        )

    def test_flattens_key_value_property_list(self):
        data = {
            "items": [
                {
                    "name": "finance",
                    "properties": [
                        {
                            "key": "uri",
                            "value": "http://catalog",
                        },
                        {
                            "key": "token",
                            "value": "********",
                        },
                    ],
                }
            ]
        }

        df = catalogs_to_dataframe(data)

        self.assertEqual(
            df["properties_uri"][0],
            "http://catalog",
        )

        self.assertEqual(
            df["properties_token"][0],
            "********",
        )

    def test_accepts_json_string(self):
        data = """
        {
            "items": [
                {
                    "name": "finance"
                }
            ]
        }
        """

        df = catalogs_to_dataframe(data)

        self.assertEqual(
            df["name"][0],
            "finance",
        )

    def test_empty_items_returns_empty_dataframe(self):
        df = catalogs_to_dataframe({"items": []})

        self.assertTrue(df.is_empty())


class TestInventoryCollection(unittest.TestCase):
    def setUp(self):
        self.environments = [
            Environment(
                name="a2",
                uri="https://a2",
                token="token-a2",
            ),
            Environment(
                name="a3",
                uri="https://a3",
                token="token-a3",
            ),
        ]

    @patch("inventory.get_json")
    def test_complete_scan(self, get_json):
        get_json.side_effect = [
            {"items": [{"name": "catalog-a"}]},
            {"items": [{"name": "catalog-b"}]},
        ]

        result = fetch_catalog_inventory(self.environments)

        self.assertEqual(
            result.status,
            "COMPLETE",
        )

        self.assertEqual(
            result.configured_count,
            2,
        )

        self.assertEqual(
            len(result.successful_environments),
            2,
        )

        self.assertEqual(
            len(result.failures),
            0,
        )

        self.assertEqual(
            len(result.inventory),
            2,
        )

    @patch("inventory.get_json")
    def test_one_failure_makes_scan_partial(self, get_json):
        get_json.side_effect = [
            {"items": [{"name": "catalog-a"}]},
            RuntimeError("boom"),
        ]

        result = fetch_catalog_inventory(self.environments)

        self.assertEqual(
            result.status,
            "PARTIAL",
        )

        self.assertEqual(
            len(result.successful_environments),
            1,
        )

        self.assertEqual(
            len(result.failures),
            1,
        )

        self.assertEqual(
            result.failures[0].environment,
            "a3",
        )

    @patch("inventory.get_json")
    def test_zero_catalog_environment_is_still_successful(
        self,
        get_json,
    ):
        get_json.side_effect = [
            {"items": []},
            {"items": [{"name": "catalog-b"}]},
        ]

        result = fetch_catalog_inventory(self.environments)

        self.assertEqual(
            result.status,
            "COMPLETE",
        )

        self.assertEqual(
            set(result.successful_environments),
            {"a2", "a3"},
        )

        self.assertEqual(
            len(result.inventory),
            1,
        )

    @patch("inventory.get_json")
    def test_all_failures_return_empty_inventory(
        self,
        get_json,
    ):
        get_json.side_effect = [
            RuntimeError("a2 failed"),
            RuntimeError("a3 failed"),
        ]

        result = fetch_catalog_inventory(self.environments)

        self.assertTrue(result.inventory.is_empty())

        self.assertEqual(
            result.status,
            "PARTIAL",
        )

        self.assertEqual(
            len(result.failures),
            2,
        )

    @patch("inventory.get_json")
    def test_environment_name_is_added_to_inventory(
        self,
        get_json,
    ):
        get_json.side_effect = [
            {"items": [{"name": "finance"}]},
            {"items": []},
        ]

        result = fetch_catalog_inventory(self.environments)

        self.assertIn(
            "env_name",
            result.inventory.columns,
        )

        self.assertEqual(
            result.inventory["env_name"][0],
            "a2",
        )

    @patch("inventory.get_json")
    def test_fetch_timestamp_is_added(
        self,
        get_json,
    ):
        get_json.side_effect = [
            {"items": [{"name": "finance"}]},
            {"items": []},
        ]

        result = fetch_catalog_inventory(self.environments)

        self.assertIn(
            "fetch_time",
            result.inventory.columns,
        )


if __name__ == "__main__":
    unittest.main()
