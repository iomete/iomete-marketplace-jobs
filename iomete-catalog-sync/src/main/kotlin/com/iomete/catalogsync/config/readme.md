# Example config file

Rules to ignore specific assets during the sync process.

```json
{
  "exclusion_rules": {
    "catalogs": {
      "names": [
        "sandbox_dev",
        "legacy_warehouse",
        "user_personal_spaces"
      ],
      "filter_by_properties": {
        "iomete.governance.index": "false"
      }
    },
    "schemas": {
      "filter_by_properties": {
        "iomete.governance.index": "false"
      }
    },
    "tables": {
      "filter_by_properties": {
        "iomete.governance.index": "false",
        "hidden": "true"
      }
    },
    "default_rule": {
      "filter_by_properties": {
        "iomete.governance.index": "false"
      }
    }
  }
}
```
