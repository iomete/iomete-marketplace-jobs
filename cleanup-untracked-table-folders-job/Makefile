image := iomete.azurecr.io/iomete/cleanup-untracked-table-folders
tag   := 0.1.5
# Pass an image digest (repository@sha256:...) to reproduce a build exactly.
spark_image := iomete.azurecr.io/iomete/spark:3.5.7-latest

.PHONY: build
build:
	./gradlew clean quarkusBuild

.PHONY: docker-push
docker-push: build
	docker buildx build --platform linux/amd64,linux/arm64 --build-arg SPARK_IMAGE=${spark_image} -f Dockerfile -t ${image}:${tag} --push . --sbom=true --provenance=true
	@echo ${image}
	@echo ${tag}

.PHONY: docker-run
docker-run:
	docker run -it --rm \
		-v ${PWD}/conf:/etc/configs \
		-e SPARK_LOCAL_IP=127.0.0.1 \
		-e QUARKUS_PROFILE=prod \
		$(image):$(tag) driver --class com.iomete.cleanup.untrackedtablefolders.App spark-internal
