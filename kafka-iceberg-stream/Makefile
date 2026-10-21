docker_image := iomete.azurecr.io/iomete/kafka-iceberg-stream
# Released by CI when VERSION changes on main, see the repo README.
docker_tag := $(shell cat VERSION)

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

