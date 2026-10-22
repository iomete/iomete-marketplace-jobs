# iomete-marketplace-jobs
This repo contains marketplace jobs that can run smoothly on IOMETE platform

## Releasing a job

A job is released by CI when its folder has a `Dockerfile` and a `VERSION` file holding one line, `<image>:<version>`:

```
iomete_data_compaction:1.2.14
```

1. In your PR, bump the version, for example to `iomete_data_compaction:1.2.15`. The PR check fails if the version is not higher, is already published, or the image name changed.
2. Merge. The image is built from the job folder and published as `iomete.azurecr.io/iomete/<image>:<version>`, and the tag is locked.
3. For a marketplace job, its version is bumped in the platform chart's marketplace ConfigMap, so the marketplace offers the new image with the next platform release.
4. The "Latest Versions" table in the docs is updated, and the commit is tagged `<image>/<version>`.

A merge that does not change `VERSION` releases nothing. Use `make docker-build` to build an image locally.

To release a new job, add its folder with a `Dockerfile` that builds from a clean checkout and a `VERSION` file. For a marketplace job, also add it to `docker.marketplaceJobs` and `bom.yaml` in infra's `iomete-data-plane-enterprise` chart and to `mirrored_marketplace_job_repos` in `terraform/iomete-development-services/argo-workflows.tf`, so dev can pull it through its internal registry.
