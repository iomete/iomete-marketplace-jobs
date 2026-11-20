# Example config file

The job reads its config from `/etc/configs/application.json`. Without that file it syncs every catalog.

- `inclusion_rules.catalogs.names`: sync only these catalogs. Leave it out, or empty, to sync every catalog.
- `exclusion_rules`: rules to ignore specific assets during the sync process. They also apply to included catalogs, so a catalog that is both included and excluded is skipped.

Catalog names match exactly, case included. An included name that matches no catalog is logged as a warning.

```json
{
  "inclusion_rules": {
    "catalogs": {
      "names": ["my_catalog"]
    }
  },
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
