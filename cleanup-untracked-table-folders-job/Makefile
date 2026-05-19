image := iomete.azurecr.io/iomete/cleanup-untracked-table-folders
tag   := 0.1.0-rc1

.PHONY: build
build:
	./gradlew clean quarkusBuild

.PHONY: docker-push
docker-push: build
	docker buildx build --platform linux/amd64,linux/arm64 -f Dockerfile -t ${image}:${tag} --push . --sbom=true --provenance=true
	@echo ${image}
	@echo ${tag}

.PHONY: docker-run
docker-run:
	docker run -it --rm \
		-v ${PWD}/conf:/etc/configs \
		-e SPARK_LOCAL_IP=127.0.0.1 \
		-e QUARKUS_PROFILE=prod \
		$(image):$(tag) driver --class com.iomete.cleanup.untrackedtablefolders.App spark-internal
