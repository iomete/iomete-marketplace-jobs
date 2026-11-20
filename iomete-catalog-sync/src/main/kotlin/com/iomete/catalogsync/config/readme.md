# Example config file

The job reads its config from `/etc/configs/application.json`. Without that file it syncs every catalog.

- `inclusion_rules.catalogs.names`: sync only these catalogs. Leave it out, or empty, to sync every catalog.
- `exclusion_rules`: rules to ignore specific assets during the sync process. They also apply to included catalogs, so a catalog that is both included and excluded is skipped.

Catalog names match exactly, case included. An included name that matches no catalog is logged as a warning, and the run fails when none of them match. A config file that cannot be parsed, or has an unknown key inside a rule, also fails the run instead of falling back to the defaults.

Taking a catalog out of the inclusion list (or adding it to the exclusion list) does not remove what was already synced for it. The data catalog only marks tables deleted in a catalog the run visits, so a catalog the job no longer visits keeps its last synced metadata, and it stays searchable.

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
