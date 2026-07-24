image := iomete.azurecr.io/iomete/iomete-lakehouse-backup
tag   := $(shell sed -n 's/^projectVersion=//p' gradle.properties)
# Optional second tag (e.g. commit SHA) for per-build traceability.
extra_tag :=

spark_version  := $(shell sed -n 's/^sparkVersion=//p' gradle.properties)
spark_revision := $(shell sed -n 's/^sparkImageRevision=//p' gradle.properties)
spark_image    := iomete.azurecr.io/iomete/spark:$(spark_version)-$(spark_revision)

.PHONY: build
build:
	./gradlew clean jar

.PHONY: docker-build
docker-build: build
	docker build --build-arg SPARK_IMAGE=$(spark_image) -f Dockerfile -t ${image}:${tag} .

.PHONY: docker-push
docker-push: build
	docker buildx build --platform linux/amd64,linux/arm64 --build-arg SPARK_IMAGE=$(spark_image) -f Dockerfile -t ${image}:${tag} --push . --sbom=true --provenance=true
	@echo ${image}:${tag}

.PHONY: docker-push-tag
docker-push-tag: build
	docker buildx build --platform linux/amd64,linux/arm64 --build-arg SPARK_IMAGE=$(spark_image) -f Dockerfile -t ${image}:${tag} -t ${image}:$(extra_tag) --push . --sbom=true --provenance=true
	@echo ${image}:${tag} ${image}:$(extra_tag)

.PHONY: docker-push-latest
docker-push-latest: build
	docker buildx build --platform linux/amd64,linux/arm64 --build-arg SPARK_IMAGE=$(spark_image) -f Dockerfile -t ${image}:latest --push . --sbom=true --provenance=true
	@echo ${image}:latest

.PHONY: test
test:
	./gradlew test

.PHONY: integration-test
integration-test:
	./gradlew integrationTest

spark_submit_cmd := /opt/spark/bin/spark-submit \
		--properties-file=/opt/spark/conf/spark-defaults.conf \
		--class com.iomete.backup.App \
		local:///opt/spark/jars/iomete-lakehouse-backup.jar \
		/etc/configs/application.json

.PHONY: docker-run
docker-run: docker-build
	docker run --rm \
		-v ${PWD}/conf:/etc/configs \
		-e SPARK_LOCAL_IP=127.0.0.1 \
		${image}:${tag} \
		$(spark_submit_cmd)