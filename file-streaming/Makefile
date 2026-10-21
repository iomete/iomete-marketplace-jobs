docker_image := iomete.azurecr.io/iomete/iomete-file-streaming
# Released by CI when VERSION changes on main, see the repo README.
docker_tag := $(shell cut -d: -f2 VERSION)

test:
	python setup.py test

docker-build:
	docker build -f Dockerfile -t ${docker_image}:${docker_tag} .
	@echo ${docker_image}:${docker_tag}
