SHELL:=/bin/bash -euxo pipefail
DOCKER_RAP:=bin/docker/raphtory
DOCKER_TMP:=$(DOCKER_RAP)/tmp
MODE:=batch

version:
	sbt -Dsbt.supershell=false -error "print core/version" > version

.PHONY sbt-build:
sbt-build: version
	sbt clean "core/assembly"

.PHONY python-build:
python-build: version
	if [ -z ${JAVA_HOME+x} ]; then echo "JAVA_HOME is unset" && exit 1; else echo "JAVA_HOME is set to '$var'"; fi
	cd python/pyraphtory/ && \
		poetry build && \
		poetry install && \
		pip install --upgrade pip
	pip install python/pyraphtory/dist/pyraphtory-$$(cat version).tar.gz

.PHONY pyraphtory-local:
pyraphtory-local: version
	java -cp core/target/scala-2.13/*.jar com.raphtory.python.PyRaphtory --input=$(INPUT) --py=$(PYFILE) --builder=$(BUILDER) --mode=$(MODE)

.PHONY: docker-build
docker-build: version
	docker build \
		--build-arg VERSION="$$(cat version)" \
		-t raphtory-os:$$(cat version) \
		-f $(DOCKER_RAP)/DockerfileV2 . --compress


.PHONY: run-local-cluster
run-local-cluster: version
	mkdir -p $(DOCKER_TMP)/builder
	mkdir -p $(DOCKER_TMP)/partition
	mkdir -p $(DOCKER_TMP)/query
	mkdir -p $(DOCKER_TMP)/spout
	curl -o $(DOCKER_TMP)/spout/lotr.csv https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv
	cp python/pyraphtory/sample.py $(DOCKER_TMP)/builder/
	VERSION=$$(cat version) docker-compose -f $(DOCKER_RAP)/docker-compose.yml up

.PHONY: run-local-cluster
clean-local-cluster:
	docker-compose -f $(DOCKER_RAP)/docker-compose.yml down --remove-orphans
	rm -Rf $(DOCKER_TMP)/*

clean:
	rm version
	sbt clean

local-pulsar: version
	VERSION=$$(cat version) docker-compose -f $(DOCKER_RAP)/docker-compose-local.yml up
