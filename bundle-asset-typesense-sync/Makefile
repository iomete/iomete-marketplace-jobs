docker_image ?= iomete.azurecr.io/iomete/bundle-asset-typesense-sync
docker_tag ?= latest
platforms ?= linux/amd64,linux/arm64

.PHONY: help run install install-dev test docker-build docker-push az-login

help:
	@echo "Common targets:"
	@echo "  make install              # install runtime dependencies"
	@echo "  make install-dev          # install runtime + dev dependencies"
	@echo "  make test                 # run pytest suite"
	@echo "  make run ARGS=...         # run the sync locally"
	@echo "  make docker-build         # build docker image for local testing"
	@echo "  make docker-push          # build and push multi-arch image"

install:
	pip install --upgrade pip
	pip install -r requirements.txt

install-dev:
	pip install --upgrade pip
	pip install -r requirements-dev.txt

test:
	PYTHONPATH=. pytest -q

run:
	python main.py $(ARGS)

docker-build:
	docker build -f docker/Dockerfile -t $(docker_image):$(docker_tag) .

az-login:
	az acr login --name iomete

docker-push: az-login
	# Run this once if needed: docker buildx create --use
	docker buildx build --platform $(platforms) -f docker/Dockerfile -t $(docker_image):$(docker_tag) --push .
