.PHONY sbt-build:
SHELL:=/bin/bash -euxo pipefail
version:
	sbt -Dsbt.supershell=false -error "print core/version" > version

sbt-build: version
	sbt clean "core/assembly"

.PHONY: docker-build
docker-build: sbt-build
	docker build --progress=plain --build-arg VERSION=$$(cat version) -t raphtory-os:$$(cat version) -f bin/docker/raphtory/DockerfileV2 . --compress
