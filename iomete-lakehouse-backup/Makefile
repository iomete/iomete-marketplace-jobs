image := iomete.azurecr.io/iomete/iomete-lakehouse-backup
# Released by CI when VERSION changes on main, see the repo README.
tag   := $(shell cat VERSION)

spark_version  := $(shell sed -n 's/^sparkVersion=//p' gradle.properties)
spark_revision := $(shell sed -n 's/^sparkImageRevision=//p' gradle.properties)
spark_image    := iomete.azurecr.io/iomete/spark:$(spark_version)-$(spark_revision)

.PHONY: build
build:
	./gradlew clean jar

.PHONY: docker-build
docker-build:
	docker build --build-arg SPARK_IMAGE=$(spark_image) -f Dockerfile -t ${image}:${tag} .

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