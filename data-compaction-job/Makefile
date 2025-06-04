docker_image := iomete.azurecr.io/iomete/iomete_data_compaction
docker_tag := 1.2.5

docker-push:
	# Run this for one time: docker buildx create --use
	docker buildx build --platform linux/amd64,linux/arm64 --push -f docker/Dockerfile -t ${docker_image}:${docker_tag} .
	@echo ${docker_image}
	@echo ${docker_tag}

install:
	poetry install -vv

# Create virtualenv using poetry and install dependencies with dev-dependencies and optional dependencies
install-with-optional:
	poetry install -vv --with optional

# Update lock and create virtualenv using poetry and install dependencies with dev-dependencies
update-install:
	poetry update -vv

# Update lock and create virtualenv using poetry and install dependencies with dev-dependencies and optional dependencies
update-install-with-optional:
	poetry update -vv --with optional

# Create virtualenv using poetry and install dependencies without dev-dependencies
install-no-dev:
	poetry install --no-dev -vv

# Create virtualenv using poetry and install dependencies without dev-dependencies
update-install-no-dev:
	poetry update --no-dev -vv

# Update pip, wheel and setuptools. This step does not assume poetry
install-build-deps:
	python -m pip install -U pip wheel setuptools

# Run formatting on the package
format: isort blue

# Run formatting checks on the package
format-check: isort-check blue-check

# Run isort formatting on the package
isort:
	poetry run isort ./

# Run isort formatting check on the package
isort-check:
	poetry run isort --check ./

# Run blue formatting on the package
blue:
	poetry run blue ./

# Run blue formatting check on the package
blue-check:
	poetry run blue --check ./

# Run package test suit using pytest
test:
	poetry run pytest ./tests

# Run package test suit using pytest
test-no-poetry:
	python -m pytest ./tests

# Run package release acceptance tests - should be custom for the package
release-acceptance-tests: test

# Run mypy check on the package

# Run documentation coverage using interrogate
interrogate:
	poetry run interrogate .

# Build the package wheel and sdist
build-all:
	poetry build -vv

# Build the package wheel (the binaries)
build-wheel:
	poetry build -f wheel -vv

# Build the package sdist
build-sdist:
	poetry build -f sdist -vv

# Removes a generic wheel that matches the *-py3-*.whl, useful for publishing packages to prevent override
remove-generic-wheel:
	rm -f dist/*-py3-*.whl

# Package and publish the docs to causaLens' artifactory
package-and-publish-docs:
	poetry source add --priority=supplemental causalens https://us-central1-python.pkg.dev/causalens-internal/python-internal/simple
	poetry add --source=causalens docs-builder@~0.2.8
	poetry run python ./tooling/scripts/docs-upload.py

# Publish the package to PyPI
publish:
	poetry config pypi-token.pypi ${PYPI_TOKEN}
	poetry publish

# Check the package can import
self-import-check:
	python -c "import data-compaction-job"

# Check the package can import
self-import-check-poetry:
	poetry run python -c "import data-compaction-job"
