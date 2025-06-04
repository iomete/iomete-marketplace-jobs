docker_image := iomete.azurecr.io/iomete/iomete_mysql_sync
docker_tag := 3.0.0

test:
	pytest --capture=no --log-cli-level=DEBUG

docker-build:
	# Run this for one time: docker buildx create --use
	docker build -f docker/Dockerfile -t ${docker_image}:${docker_tag} .
	@echo ${docker_image}
	@echo ${docker_tag}

docker-push:
	# Run this for one time: docker buildx create --use
	az acr login --name iomete
	docker buildx build --platform linux/amd64,linux/arm64 --push -f docker/Dockerfile -t ${docker_image}:${docker_tag} . --sbom=true --provenance=true
	az acr repository update --name iomete --image iomete/iomete_mysql_sync:${docker_tag} --write-enabled false --delete-enabled false
	@echo ${docker_image}
	@echo ${docker_tag}