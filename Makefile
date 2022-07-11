.PHONY sbt-build:
SHELL:=/bin/bash -euxo pipefail
version:
	sbt -Dsbt.supershell=false -error "print core/version" > version

sbt-build:
	sbt clean "core/assembly"

.PHONY: docker-build
docker-build: version
	docker build --progress=plain --build-arg VERSION=$$(cat version) -t raphtory-os:$$(cat version) -f bin/docker/raphtory/DockerfileV2 . --compress
