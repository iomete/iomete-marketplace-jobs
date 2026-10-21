# iomete-marketplace-jobs
This repo contains marketplace jobs that can run smoothly on IOMETE platform

## Releasing a job

Jobs listed in `marketplace-jobs.json` are released by CI. Each keeps its version in a `VERSION` file in its folder.

1. In your PR, bump the job's `VERSION`, for example `1.2.14` to `1.2.15`. The PR check fails if the version is not higher or is already published.
2. Merge. The image is built and published as `iomete.azurecr.io/iomete/<job>:<version>`, and the tag is locked.
3. The job's version is bumped in the platform chart's marketplace ConfigMap, so the marketplace offers the new image. It reaches customers with the next platform release.
4. The released commit is tagged `<job>/<version>`.

A merge that does not change `VERSION` releases nothing. Use `make docker-build` to build an image locally.

To release another job this way, add it to `marketplace-jobs.json` with a `VERSION` file, and add the job to `docker.marketplaceJobs` and `bom.yaml` in infra's `iomete-data-plane-enterprise` chart. Its Dockerfile has to build from a clean checkout, with no build steps beforehand.
