docker_image := iomete/kinesis-streaming-job
docker_tag := 0.1.0

export SPARK_CONF_DIR=./spark_conf
export APPLICATION_CONFIG_FILE=application-dev.conf

install-dev-requirements:
	pip install -r infra/requirements-dev.txt

run-job:
	python job.py

run-tests:
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
