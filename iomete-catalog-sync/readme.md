# Configuration

The job reads an optional config from `/etc/configs/application.json` (mount it as a ConfigMap in the job's configuration). Without it, every catalog is synced, apart from assets with the `iomete.governance.index=false` property.

```json
{
  "inclusion_rules": {
    "catalogs": {
      "names": ["my_catalog"]
    }
  },
  "exclusion_rules": {
    "catalogs": {
      "names": ["sandbox_dev"],
      "filter_by_properties": { "iomete.governance.index": "false" }
    },
    "schemas": {
      "filter_by_properties": { "iomete.governance.index": "false" }
    },
    "tables": {
      "filter_by_properties": { "hidden": "true" }
    }
  }
}
```

- `inclusion_rules.catalogs.names`: sync only these catalogs. Leave it out, or empty, to sync every catalog.
- `exclusion_rules.catalogs.names`: never sync these catalogs.
- `filter_by_properties`: skip a catalog, schema or table when any of the listed properties has the given value.
- `exclusion_rules.default_rule.filter_by_properties`: applies to catalogs, schemas and tables alike. Defaults to `{"iomete.governance.index": "false"}`.

## How inclusion and exclusion combine

1. The inclusion list narrows the catalogs to sync. In a private domain, the names are checked against the catalogs the domain owns and the job was launched with.
2. Every exclusion rule then still runs on what is left: `catalogs.names`, `catalogs.filter_by_properties` and `default_rule`.
3. Exclusion always wins. A catalog that is in the inclusion list and also matches any exclusion rule is not synced, and the run logs a warning naming the rule.

For example, an included catalog with the `iomete.governance.index=false` property is never synced, because the default rule excludes it.

## Failures

Catalog names match exactly, case included. The run fails in these cases instead of syncing with the defaults:

- None of the included names match a catalog. If only some of them are unknown, the run logs a warning and syncs the rest.
- The config file cannot be parsed, for example because it is not valid JSON (`//` comments are not allowed) or has an unknown key inside a rule, such as `"schemas": {"names": [...]}` or camelCase `filterByProperties`. Unknown top-level keys are ignored.

An empty config file is treated like a missing one.

Taking a catalog out of the inclusion list (or adding it to the exclusion list) does not remove what was already synced for it. The data catalog only marks tables deleted in a catalog the run visits, so a catalog the job no longer visits keeps its last synced metadata, and it stays searchable.

# Running Locally

```bash
# 1, Build code
../gradlew clean quarkusBuild

# 2. Build the image locally. Releases come from bumping VERSION, see the repo README.
make docker-build
```

### 1. Setup IOMETE Spark Folder
```shell
make download-spark
make download-libraries
```

### 2. Port Forward Metastore pod from dev kubernetes

### 3. Generate Random Tables
```shell
make spark-sql

---
use iceberg_catalog;
create database icedb;
use icedb;

create table sample_tbl(id int);
insert into sample_tbl (id) values(1);
select * from sample_tbl;
```

### 4. Setup Mock Server
```shell
python3 mock.py
```

### 5. Submit your jar
```shell
make submit-local
```
