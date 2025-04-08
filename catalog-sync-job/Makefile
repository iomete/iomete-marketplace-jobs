image := iomete.azurecr.io/iomete/iom-catalog-sync
tag   := 4.2.0

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

.PHONY: build
build:
	../gradlew clean quarkusBuild

.PHONY: docker-push
docker-push: build
	docker buildx build --platform linux/amd64,linux/arm64 -f Dockerfile -t ${image}:${tag} --push . --sbom=true --provenance=true
	@echo ${image}
	@echo ${tag}


docker-run:
	docker run -it --rm \
		-v ${PWD}/conf:/opt/spark/conf \
		-v ${PWD}/lakehouse:/lakehouse \
		-e SPARK_LOCAL_IP=127.0.0.1 \
		-e QUARKUS_PROFILE=prod \
		$(image):$(tag) driver --class com.iomete.catalogsync.App spark-internal


spark := ./../spark/spark-3.5.3-iomete-v5-dist

.PHONY: submit-local
submit-local:
	CORE_SERVICE_MP_REST_URL=http://127.0.0.1:3000 \
	IOMETE_RELEASE_NAMESPACE=iomete-system \
	IOMETE_CLUSTER_DNS=cluster.local \
	IOMETE_DOMAIN=default \
	IOMETE_CORE_SERVICE_URL=http://127.0.0.1:3000 \
	$(spark)/bin/spark-submit \
	--properties-file=conf/spark-defaults.conf \
	--class com.iomete.catalogsync.App \
	build/catalog-sync-job-2.1.0-runner.jar
