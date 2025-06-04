docker_tag := 3.5.4-v2
repo := iomete.azurecr.io/iomete

SPARK_VERSION := 3.5.3

s3_location := s3://iomete-config/spark/$(SPARK_VERSION)
spark_dist := spark-$(SPARK_VERSION)-iomete-v5-dist

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
	APPLICATION_CATALOG_ENDPOINT=http://localhost:8080 \
	CORE_SERVICE_MP_REST_URL=http://127.0.0.1:3000 \
	IOMETE_RELEASE_NAMESPACE=iomete-system \
	IOMETE_CLUSTER_DNS=cluster.local \
	IOMETE_DOMAIN=default \
	IOMETE_CORE_SERVICE_URL=http://127.0.0.1:3000 \
	./spark/$(spark_dist)/bin/spark-submit \
	--properties-file=catalog-sync-job/conf/spark-defaults.conf \
	--class com.iomete.catalogsync.App \
	catalog-sync-job/build/catalog-sync-job-2.1.0-runner.jar
