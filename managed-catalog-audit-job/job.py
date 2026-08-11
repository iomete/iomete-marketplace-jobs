import json
import urllib.request
import polars as pl
import os


pl.Config.set_fmt_str_lengths(1000)  # full string values
pl.Config.set_tbl_cols(-1)           # all columns
pl.Config.set_tbl_rows(-1)           # all rows


def get(base, path, token):
    req = urllib.request.Request(
        base.rstrip("/") + path,
        headers={"Authorization": f"Bearer {token}"}
    )
    return json.loads(urllib.request.urlopen(req, timeout=60).read())


def json_to_df(data):
    if isinstance(data, str):
        data = json.loads(data)

    rows = []
    for item in data["items"]:
        row = {}
        for k, v in item.items():
            if isinstance(v, dict):
                for dk, dv in v.items():
                    row[f"{k}_{dk}"] = dv
            elif isinstance(v, list):
                for entry in v:
                    if isinstance(entry, dict) and "key" in entry and "value" in entry:
                        row[f"{k}_{entry['key']}"] = entry["value"]
            else:
                row[k] = v
        rows.append(row)

    return pl.DataFrame(rows, infer_schema_length=len(rows))


def fetch_all(csv_path):
    environments = pl.read_csv(csv_path)  # expects columns: name, uri, token

    all_dfs = []

    for row in environments.iter_rows(named=True):
        name, base, token = row["name"], row["uri"], row["token"]
        try:
            response = get(base, "/api/v1/admin/spark/settings/catalogs", token)
            df = json_to_df(response)
            df = df.with_columns(pl.lit(name).alias("env_name"))
            all_dfs.append(df)
            print(f"✓ {name}: {len(df)} catalogs")
        except Exception as e:
            print(f"✗ {name}: ERROR - {e}")

    combined = pl.concat(all_dfs, how="diagonal")
    combined = combined.with_columns(pl.lit(str(pl.Series([__import__('datetime').datetime.now()])[0])).alias("fetch_time"))
    return combined


# Usage
# get env variables from env using os.getenv
# Example: os.getenv("ENV_VAR_NAME")

df = fetch_all("envs.csv")



# ---------------------------------------------------------------------------
# 1: INTERNALs in multiple clusters
# ---------------------------------------------------------------------------

df1 = (
    df
    .select([
        'env_name', 'name', 'type',
        'catalogType_type', 'catalogType_classification',
        'lakehouseDir', 'properties_uri',
        'credentials_accessKey', 'credentials_endpoint',
    ])
    .filter(
        (pl.col('catalogType_type') == 'iceberg') &
        (pl.col('catalogType_classification') == 'internal')
    )
    .drop(['catalogType_type', 'catalogType_classification'])
    .rename({
        'name': 'catalog',
        'lakehouseDir': 'location',
        'properties_uri': 'storage_link',
        'env_name': 'cluster',
        'credentials_endpoint': 'host',
        'credentials_accessKey': 'access_key',
    })
)


# Count distinct values in each column
print(df1.select(pl.all().n_unique()))

# Validate: each (location, host, access_key) belongs to at most one cluster
result = (
    df1
    .group_by(['location', 'host', 'access_key'])
    .agg(pl.col('cluster').n_unique().alias('n_clusters'))
    .filter(pl.col('n_clusters') > 1)
)
print(result)

# Validate: each (catalog, location, host) belongs to at most one cluster
result = (
    df1
    .group_by(['catalog', 'location', 'host'])
    .agg(pl.col('cluster').n_unique().alias('n_clusters'))
    .filter(pl.col('n_clusters') > 1)
)
print(result)

# Catalogs (excl. spark_catalog) shared across multiple clusters by (catalog, location, host)
result = (
    df1
    .filter(pl.col('catalog') != 'spark_catalog')
    .group_by(['catalog', 'location', 'host'])
    .agg([
        pl.col('cluster').n_unique().alias('n_clusters'),
        pl.col('cluster').unique().sort().alias('clusters'),
    ])
    .filter(pl.col('n_clusters') > 1)
    .drop('n_clusters')
    .rename({'location': 'bucket'})
)
print(result)

# Buckets shared across multiple clusters by (location, host, access_key)
result = (
    df1
    .filter(pl.col('catalog') != 'spark_catalog')
    .group_by(['location', 'host', 'access_key'])
    .agg([
        pl.col('cluster').n_unique().alias('n_clusters'),
        pl.col('cluster').unique().sort().alias('clusters'),
        pl.col('catalog').unique().sort().alias('catalogs'),
    ])
    .filter(pl.col('n_clusters') > 1)
    .drop('n_clusters')
    .rename({'location': 'bucket'})
)
print(result)

# Catalogs shared across clusters by (catalog, location, host, access_key)
result = (
    df1
    .filter(pl.col('catalog') != 'spark_catalog')
    .group_by(['catalog', 'location', 'host', 'access_key'])
    .agg([
        pl.col('cluster').n_unique().alias('n_clusters'),
        pl.col('cluster').unique().sort().alias('clusters'),
    ])
    .filter(pl.col('n_clusters') > 1)
    .drop('n_clusters')
    .rename({'location': 'bucket'})
)
print(result)


# ---------------------------------------------------------------------------
# 2: Access keys
# Catalogs with same location+host in multiple clusters but different access keys
# ---------------------------------------------------------------------------

df2 = (
    df
    .select([
        'env_name', 'name', 'type',
        'catalogType_type', 'catalogType_classification',
        'lakehouseDir', 'properties_uri',
        'credentials_accessKey', 'credentials_endpoint',
    ])
    .rename({
        'name': 'catalog',
        'lakehouseDir': 'location',
        'properties_uri': 'storage_link',
        'env_name': 'cluster',
        'credentials_endpoint': 'host',
        'credentials_accessKey': 'access_key',
    })
)

result = (
    df2
    .with_columns(pl.col('cluster').str.replace_all(r'\s+', '_'))
    .filter(pl.col('catalog') != 'spark_catalog')
    .group_by(['catalog', 'location', 'host'])
    .agg([
        pl.col('cluster').n_unique().alias('n_clusters'),
        pl.col('access_key').n_unique().alias('n_access_keys'),
        pl.col('cluster').unique().sort().alias('clusters'),
        pl.col('access_key').unique().sort().alias('access_keys'),
    ])
    .filter(
        (pl.col('n_clusters') > 1) & (pl.col('n_access_keys') > 1)
    )
    .drop(['n_clusters', 'n_access_keys'])
    .rename({'location': 'bucket'})
)
print(result)
