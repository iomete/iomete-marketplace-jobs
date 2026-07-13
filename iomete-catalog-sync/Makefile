image := iomete.azurecr.io/iomete/iom-catalog-sync
tag   := 5.0.3

.PHONY: build
build:
	./gradlew clean quarkusBuild

.PHONY: integration-test
integration-test:
	./gradlew integrationTest

.PHONY: docker-push
docker-push: build
	docker buildx build --platform linux/amd64,linux/arm64 -f Dockerfile -t ${image}:${tag} --push . --sbom=true --provenance=true
	@echo ${image}
	@echo ${tag}

spark_submit_cmd := /opt/spark/bin/spark-submit \
			--properties-file=/opt/spark/conf/spark-defaults.conf \
			--class com.iomete.LakehouseMetadataSyncMain \
			local:///build/lakehouse-metadata-sync-2.1.0-runner.jar

spark-submit:
	docker run \
			-v ${PWD}/build:/build -v ${PWD}/docker_files:/opt/spark/conf \
			-e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
			-e PRESIDIO_MP_REST_URL=http://host.docker.internal:5001 \
           	-e APPLICATION_DATA_CATALOG_SERVICE=http://host.docker.internal:8084 \
           	-e APPLICATION_INCLUDE_SCHEMAS=tpcds10test \
			iomete/spark:3.3.3-latest \
			$(spark_submit_cmd)

docker-run:
	docker run -it --rm \
		-v ${PWD}/conf:/opt/spark/conf \
		-v ${PWD}/lakehouse:/lakehouse \
		-e SPARK_LOCAL_IP=127.0.0.1 \
		-e QUARKUS_PROFILE=prod \
		$(image):$(tag) driver --class com.iomete.catalogsync.App spark-internal


docker_tag := 3.5.4-v2
repo := iomete.azurecr.io/iomete

SPARK_VERSION := 3.5.3

s3_location := s3://iomete-config/spark/$(SPARK_VERSION)
spark_dist := spark-$(SPARK_VERSION)-iomete-v5-dist

spark := ./../spark/spark-3.5.3-iomete-v5-dist

s3_jars_location := s3://iomete-config/spark/jars
jars := spark/$(spark_dist)/jars

download-spark:
	mkdir -p spark
	aws s3 cp $(s3_location)/$(spark_dist).zip spark/
	unzip spark/$(spark_dist).zip -d spark/
	rm -f spark/$(spark_dist).zip

download-libraries:
	aws s3 cp $(s3_location)/iceberg-spark-runtime-3.5_2.12-1.6.1.jar $(jars)
	aws s3 cp $(s3_location)/iceberg-aws-bundle-1.6.1.jar $(jars)

	wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.2/postgresql-42.7.2.jar -P $(jars)
	wget https://repo1.maven.org/maven2/org/apache/spark/spark-hadoop-cloud_2.12/$(SPARK_VERSION)/spark-hadoop-cloud_2.12-$(SPARK_VERSION).jar -P $(jars)

	# AWS Libs
	aws s3 cp $(s3_jars_location)/aws-java-sdk-core-1.12.445.jar $(jars)
	aws s3 cp $(s3_jars_location)/aws-java-sdk-s3-1.12.445.jar $(jars)
	aws s3 cp $(s3_jars_location)/aws-java-sdk-sts-1.12.445.jar $(jars)
	aws s3 cp $(s3_jars_location)/aws-java-sdk-dynamodb-1.12.445.jar $(jars)
	aws s3 cp $(s3_jars_location)/hadoop-aws-3.3.1.jar $(jars)
	aws s3 cp $(s3_jars_location)/url-connection-client-2.21.21.jar $(jars)

	# Avro Lib
	aws s3 cp $(s3_jars_location)/spark-avro_2.12-3.5.0.jar $(jars)

#minimal test configuration (move ranger-spark-(audit|security).xml files under conf folder
spark-sql:
	# make sure you're running mock-core-service (see the corresponding folder)
	IOMETE_DOMAIN=default \
	IOMETE_SCHEMA_SERVICE_URL=http://127.0.0.1:3000 \
	IOMETE_CORE_SERVICE_URL=http://127.0.0.1:3000 \
	./spark/$(spark_dist)/bin/spark-sql --properties-file=catalog-sync-job/conf/spark-defaults.conf

.PHONY: submit-local
submit-local:
	CATALOG_SERVICE_MP_REST_URL=http://127.0.0.1:8080 \
	CORE_SERVICE_MP_REST_URL=http://127.0.0.1:3000 \
	IOMETE_RELEASE_NAMESPACE=iomete-system \
	IOMETE_CLUSTER_DNS=cluster.local \
	IOMETE_DOMAIN=default \
	IOMETE_CORE_SERVICE_URL=http://127.0.0.1:3000 \
	$(spark)/bin/spark-submit \
	--properties-file=conf/spark-defaults.conf \
	--class com.iomete.catalogsync.App \
	build/catalog-sync-job-2.1.0-runner.jar
