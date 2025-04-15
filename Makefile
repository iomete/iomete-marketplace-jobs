docker_image := iomete.azurecr.io/iomete/kafka-iceberg-stream
docker_tag := 1.2.0

export APP_CONFIG_PATH=application.conf
export SPARK_CONF_DIR=./spark_conf

install-dev-requirements:
	pip install -r infra/requirements-dev.txt

run-job:
	python job.py

tests:
	# run all tests
	pytest
	
docker-build:
	# Run this for one time: docker buildx create --use
	docker build -f infra/Dockerfile -t ${docker_image}:${docker_tag} .
	@echo ${docker_image}
	@echo ${docker_tag}

docker-push:
	# Run this for one time: docker buildx create --use
	docker buildx build --platform linux/amd64,linux/arm64 --push -f infra/Dockerfile -t ${docker_image}:${docker_tag} .
	@echo ${docker_image}
	@echo ${docker_tag}