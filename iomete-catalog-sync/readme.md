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
- `exclusion_rules.catalogs.names`: never sync these catalogs. Exclusion also applies to included catalogs, so a catalog in both lists is skipped.
- `filter_by_properties`: skip a catalog, schema or table when any of the listed properties has the given value.
- `exclusion_rules.default_rule.filter_by_properties`: applies to catalogs, schemas and tables alike. Defaults to `{"iomete.governance.index": "false"}`.

Catalog names match exactly, case included. An included name that matches no catalog is logged as a warning. If the file cannot be parsed, the job logs the error and syncs with the defaults.

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
