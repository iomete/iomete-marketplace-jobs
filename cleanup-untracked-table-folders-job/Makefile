image := iomete.azurecr.io/iomete/cleanup-untracked-table-folders
# Released by CI when VERSION changes on main, see the repo README.
tag   := $(shell cut -d: -f2 VERSION)
# Pass an image digest (repository@sha256:...) to reproduce a build exactly.
spark_image := iomete.azurecr.io/iomete/spark:3.5.7-latest

.PHONY: docker-build
docker-build:
	docker build --build-arg SPARK_IMAGE=${spark_image} -f Dockerfile -t ${image}:${tag} .
	@echo ${image}:${tag}

.PHONY: build
build:
	./gradlew clean quarkusBuild

.PHONY: docker-run
docker-run:
	docker run -it --rm \
		-v ${PWD}/conf:/etc/configs \
		-e SPARK_LOCAL_IP=127.0.0.1 \
		-e QUARKUS_PROFILE=prod \
		$(image):$(tag) driver --class com.iomete.cleanup.untrackedtablefolders.App spark-internal
