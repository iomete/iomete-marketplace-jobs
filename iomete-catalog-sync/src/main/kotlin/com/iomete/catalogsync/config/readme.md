# Example config file

The job reads its config from `/etc/configs/application.json`. Without that file it syncs every catalog.

- `inclusion_rules.catalogs.names`: sync only these catalogs. Leave it out, or empty, to sync every catalog.
- `exclusion_rules`: rules to ignore specific assets during the sync process.

The inclusion list narrows the catalogs first. Every exclusion rule (`catalogs.names`, `catalogs.filter_by_properties` and `default_rule`) then still runs, so exclusion always wins. For example, outside private domains, an included catalog with `iomete.governance.index=false` is never synced, and the run logs a warning about it.

**Private domains:** catalog properties are not available to the job there, so catalog-level property rules (`catalogs.filter_by_properties`, and `default_rule` at catalog level) never match. In a private domain, an included catalog with `iomete.governance.index=false` **is** synced, and no warning is logged. To skip a catalog there, list it in `exclusion_rules.catalogs.names`. Schema and table property rules still work, because they read the schema's and table's own properties.

Catalog names match exactly, case included. An included catalog this job cannot sync (unknown, or in a private domain not owned or not launched with) is logged as a warning, and the run fails when none of them can be synced. A config file that cannot be parsed, or has an unknown key inside a rule, also fails the run instead of falling back to the defaults. An empty file is treated like a missing one.

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
