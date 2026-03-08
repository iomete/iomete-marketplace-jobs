# Bundle Asset → Typesense Sync Job

This marketplace job backfills the bundle asset search index that lives in
Typesense. It reads the authoritative bundle/asset mapping from the IAM
PostgreSQL database, fetches display names for every asset by calling the
same downstream services that IAM talks to (compute, spark, namespaces,
etc.), and finally upserts one Typesense document per `(bundle, asset)`
pair plus a marker record per bundle so that the IAM service can detect
which bundles are already indexed.

## Runtime Requirements

The job is a plain Python script (3.12+) and depends on `requests`,
`psycopg`, `pydantic`, and `pyhocon`. Create an isolated environment before
installing anything (matches our other marketplace jobs):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r bundle-asset-typesense-sync/requirements.txt
```

## Configuration

All credentials and endpoints live in an application config file (HOCON format),
matching the approach used by other marketplace jobs. By default the job looks
for `/etc/configs/application.conf` and falls back to `application.conf` in the
current working directory. You can override the path via the
`BUNDLE_ASSET_SYNC_CONFIG` environment variable if needed.

Example `application.conf`:

```hocon
bundle_asset_typesense_sync {
  db {
    host = "iam-db"
    port = 5432
    user = "iam"
    password = "secret"
    name = "iam_db"
  }
  cluster_service {
    base_url = "http://iom-cluster"
    token = "service-token"
  }
  sql_service {
    base_url = "http://iom-sql"
    token = "service-token"
  }
  typesense {
    base_url = "http://typesense:8108"
    api_key = "typesense-key"
    collection = "bundle_assets"
    timeout_seconds = 10.0
  }
  batch_size = 1000
  http_timeout_seconds = 10.0
}
```

By default the job performs a full refresh of every active bundle. Add `--dry-run`
to skip writing any Typesense documents (the job still talks to IAM DB and downstream
services, which is useful for debugging).

## Running locally

Create an `application.conf` like the example above (either in the project root
or `/etc/configs/application.conf`) and then run:

```bash
python bundle-asset-typesense-sync/main.py
```

## Running tests

Always execute tests inside the virtual environment so dependencies stay local:

```bash
cd bundle-asset-typesense-sync
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
make test
```

## Running the job

With the virtual environment active and `application.conf` populated, run a full
sync or a read-only dry-run:

```bash
source .venv/bin/activate
python main.py          # full sync
python main.py --dry-run  # read-only validation
```

Typically you would run the script as a Kubernetes CronJob where the IAM
database credentials, Typesense key and internal service token are injected
as secrets.

## Docker image

This job ships with a container image definition similar to the other marketplace
jobs (see `ras-onboarding` or `orphaned-asset-detection`). Build the image locally:

```bash
cd bundle-asset-typesense-sync
make docker-build docker_tag=local
```

Run it by passing the expected environment variables (an `.env` file works well):

```bash
docker run --rm \
  -v $PWD/application.conf:/etc/configs/application.conf:ro \
  iomete.azurecr.io/iomete/bundle-asset-typesense-sync:local
```

The provided `Makefile` also includes `docker-push` (logs into the IOMETE registry
and pushes the freshly built image) plus a simple `run` target for local execution.

## Output / error handling

* Typesense collections are created automatically if they don’t exist.
* Each bundle is processed independently: the job deletes previously indexed
  documents for that bundle and upserts the freshly fetched set. Failures for
  a single bundle are logged and the job keeps going.
* Missing assets (e.g. an asset was deleted in the source service) are
  logged and skipped; their bundle entry will not be written which prevents
  dangling search hits.
