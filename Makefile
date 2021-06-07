#!make
SHELL=/bin/bash

.PHONY: version
version:
	[ -z "${VERSION}" ] &&  { echo "VERSION variable must be set"; exit 1; } || echo "Setting verstion to ${VERSION}"

.PHONY: build-version-migration
build-version-migration: version
	cat sql/*.sql <(echo "INSERT INTO migrations (version) VALUES ('${VERSION}');") >pypgstac/pypgstac/migrations/pgstac.${VERSION}.sql

.PHONY: build-pypgstac
build-pypgstac: version build-version-migration
	cd pypgstac; \
	echo "__version__ = '${VERSION}'" >pypgstac/__init__.py; \
	sed -i "/^version/c\version = \"${VERSION}\"" pyproject.toml; \
	sed -i "/^include/c\include = [\"pypgstac/migrations/pgstac*${VERSION}.sql\"]" pyproject.toml; \
	poetry build; \
	tar -xvf dist/pypgstac-${VERSION}.tar.gz --no-anchored 'setup.py' -O  > setup.py

.PHONY: install-pypgstac
install-pypgstac: build-pypgstac
	cd pypgstac; \
	pip install -U dist/pypgstac-${VERSION}-py3-none-any.whl; \

.PHONY: publish-pypgstac
publish-pypgstac: build-pypgstac
	cd pypgstac; \
	poetry publish

.PHONY: docker-repo
docker-repo:
	[ -z "${DOCKER_REPO}" ] &&  { echo "DOCKER_REPO variable must be set"; exit 1; } || echo "Setting verstion to ${DOCKER_REPO}"

.PHONY: build-docker
build-docker: version docker-repo
	docker build --no-cache . -t ${DOCKER_REPO}/pgstac:${VERSION}

.PHONY: push-docker
push-docker: build-docker
	docker push ${DOCKER_REPO}/pgstac:${VERSION}

.PHONY: build
build: build-pypgstac build-docker

.PHONY: publish
publish: publish-pypgstac push-docker
