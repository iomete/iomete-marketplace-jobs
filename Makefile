export SPARK_CONF_DIR=./spark_conf
export APPLICATION_CONFIG_PATH=application.conf

install-dev-requirements:
	pip install -r infra/requirements-dev.txt

run:
	python test_job.py

tests:
	# run all tests
	pytest


build-dependencies:
	cd infra && ./deps_builder.sh && rm -rf dependencies
