SHELL:=/bin/bash -euxo pipefail
DOCKER_RAP:=bin/docker/raphtory
DOCKER_TMP:=$(DOCKER_RAP)/tmp
MODE:=batch
IVY_VERSION:=2.5.1
export JAVA_HOME:= $(shell (readlink -f  $$(which java) 2>/dev/null || echo "$$(which java)")| sed "s:/bin/java::")

# version:
# 	sbt -Dsbt.supershell=false -error "exit" && \
# 	sbt -Dsbt.supershell=false -error "print core/version" | tr -d "[:cntrl:]"  > version

.PHONY: log-level-error
log-level-error:
	echo "core / Compile / logLevel := Level.Error" >> build.sbt
	echo "deploy / Compile / logLevel := Level.Error" >> build.sbt


.PHONY: sbt-build
sbt-build: version
	sbt core/makeIvyXml core/package arrowCore/makeIvyXml arrowCore/package arrowMessaging/makeIvyXml arrowMessaging/package
	mkdir -p python/_custom_build/ivy_data
	cp core/target/scala-2.13/ivy-$$(cat version).xml python/_custom_build/ivy_data/core_ivy.xml
	sed -i.bak '/org="com.raphtory"/d' python/_custom_build/ivy_data/core_ivy.xml
	cp arrow-core/target/scala-2.13/ivy-$$(cat version).xml python/_custom_build/ivy_data/arrow-core_ivy.xml
	sed -i.bak '/org="com.raphtory"/d' python/_custom_build/ivy_data/arrow-core_ivy.xml
	cp arrow-messaging/target/scala-2.13/ivy-$$(cat version).xml python/_custom_build/ivy_data/arrow_messaging_ivy.xml
	sed -i.bak '/org="com.raphtory"/d' python/_custom_build/ivy_data/arrow_messaging_ivy.xml
	mkdir -p python/pyraphtory/lib
	cp core/target/scala-2.13/core_2.13-$$(cat version).jar python/pyraphtory/lib/core.jar
	cp arrow-core/target/scala-2.13/arrow-core_2.13-$$(cat version).jar python/pyraphtory/lib/arrow-core.jar
	cp arrow-messaging/target/scala-2.13/arrow-messaging_2.13-$$(cat version).jar python/pyraphtory/lib/arrow-messaging.jar


.PHONY: gh-sbt-build
gh-sbt-build: version log-level-error sbt-build


.PHONY: sbt-skip-build
sbt-skip-build: version
	ivy-clean-copy-jars

.PHONY: sbt-thin-build
sbt-thin-build: version clean sbt-build


.PHONY: python-build
python-build: version
	python -m pip install .


.PHONY: python-dist
python-dist: python-dist-clean
	python -m pip install -q build
	python -m build


.PHONY: python-dist-clean
python-dist-clean:
	rm -rf dist
	rm -rf build
	rm -rf python/*.egg-info


.PHONY: sbt-build-clean
sbt-build-clean:
	rm -rf python/pyraphtory/lib/
	rm -rf python/_custom_build/ivy_data/
	rm -rf python/pyraphtory/jre/
	rm -rf python/pyraphtory/ivy/


.PHONY: python-build-quick
python-build-quick: version python-build

.PHONY: docs
docs: version python-build
	pip install -q myst-parser sphinx-rtd-theme sphinx docutils sphinx-tabs
	cd docs && make html


.PHONY: pyraphtory-local
pyraphtory-local: version
	java -cp core/target/scala-2.13/*.jar com.raphtory.python.PyRaphtory --input=$(INPUT) --py=$(PYFILE) --builder=$(BUILDER) --mode=$(MODE)

.PHONY: docker-build
docker-build: version
	docker build \
		--build-arg VERSION="$$(cat version)" \
		-t raphtory-core-it:$$(cat version) \
		-f Dockerfile . --compress
	docker tag raphtory-core-it:$$(cat version) raphtory-core-it:latest

.PHONY: docker-compose-up
docker-compose-up: version
	docker-compose -f docker-compose.yml up -d

.PHONY: docker-compose-down
docker-compose-down: version
	docker-compose -f docker-compose.yml down --remove-orphans

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

.PHONY: clean
clean: python-dist-clean sbt-build-clean
	sbt clean
	rm -Rf python/pyraphtory/lib/*



type?=patch
.PHONY: version-bump
version-bump:
	echo "Bumping pyraphtory version using poetry"
	cd python/pyraphtory && poetry version $(type) --short && poetry update
	echo "Bumping SBT file"
	cd python/pyraphtory && poetry version --short | tr -d "[:cntrl:]" > ../../version
	echo "Installing and bumping pyraphtory_jvm"
	pip install bump2version --quiet
	cd python/pyraphtory_jvm && bump2version --allow-dirty --no-commit --new-version $$(cat ../../version) setup.py
	cd python/pyraphtory && poetry update


.PHONY: release
release: version-bump
	git checkout -b v$$(cat version)
	git add version python/pyraphtory/pyproject.toml python/pyraphtory/poetry.lock python/pyraphtory_jvm/.bumpversion.cfg python/pyraphtory_jvm/setup.py
	git commit -m "bumped to v$$(cat version)"
	git tag "v$$(cat version)" && git push origin --tags


local-pulsar: version
	VERSION=$$(cat version) docker-compose -f $(DOCKER_RAP)/docker-compose-local.yml up

.PHONY: scala-test
scala-test:
	export RAPHTORY_CORE_LOG="ERROR" && sbt test

.PHONY: scala-remote-test
scala-remote-test:
	sleep 5
	export RAPHTORY_CORE_LOG=ERROR && \
	export RAPHTORY_ITEST_PATH=./it/target/scala-2.13/test-classes && \
	sbt "it/testOnly *.algorithms.*"


.PHONY: setup-python-env
setup-python-env:
	python -m pip install --upgrade pip
	python -m pip install -q poetry nbmake tox pytest-xdist build pyvis


.PHONY: setup-python
setup-python: gh-sbt-build setup-python-env python-build-quick


.PHONY: python-test
python-test:
	cd python/pyraphtory_jvm && tox -p -o -vv
	cd python/pyraphtory && poetry run pytest -n=auto
	cd examples && pytest --nbmake -n=auto
