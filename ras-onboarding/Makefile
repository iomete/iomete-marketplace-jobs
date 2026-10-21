docker_image := iomete.azurecr.io/iomete/ras-onboarding
# Released by CI when VERSION changes on main, see the repo README.
docker_tag := $(shell cat VERSION)

test:
	pytest --capture=no --log-cli-level=DEBUG

test-namespace:
	pytest tests/namespace/ -v

install-dev-requirements:
	pip install -e ".[dev]"

run:
	python driver.py

docker-build:
	docker build -f docker/Dockerfile -t ${docker_image}:${docker_tag} .
	@echo ${docker_image}:${docker_tag}
