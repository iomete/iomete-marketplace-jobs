image := iomete.azurecr.io/iomete/iomete-lakehouse-backup
tag   := 1.0.0

.PHONY: build
build:
	./gradlew clean jar

.PHONY: docker-build
docker-build: build
	docker build -f Dockerfile -t ${image}:${tag} .

.PHONY: docker-push
docker-push: build
	docker buildx build --platform linux/amd64,linux/arm64 -f Dockerfile -t ${image}:${tag} --push . --sbom=true --provenance=true
	@echo ${image}
	@echo ${tag}

.PHONY: test
test:
	./gradlew test

.PHONY: integration-test
integration-test:
	./gradlew integrationTest

spark_submit_cmd := /opt/spark/bin/spark-submit \
		--properties-file=/opt/spark/conf/spark-defaults.conf \
		--class com.iomete.backup.App \
		local:///opt/spark/jars/iomete-lakehouse-backup-${tag}.jar \
		/etc/configs/application.json

.PHONY: docker-run
docker-run: docker-build
	docker run --rm \
		-v ${PWD}/conf:/etc/configs \
		-e SPARK_LOCAL_IP=127.0.0.1 \
		${image}:${tag} \
		$(spark_submit_cmd)