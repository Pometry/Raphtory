SHELL:=/bin/bash -euxo pipefail
DOCKER_RAP:=bin/docker/raphtory
DOCKER_TMP:=$(DOCKER_RAP)/tmp
MODE:=batch
PYRAPHTORY_LIBDIR:=python/src/pyraphtory/lib
PYRAPHTORY_IVYDIR:=python/_custom_build/ivy_data
PYRAPHTORY_IVYBIN:=python/src/pyraphtory/ivy
PYRAPHTORY_JREBIN:=python/src/pyraphtory/jre
IVY_VERSION:=2.5.1


.PHONY: log-level-error
log-level-error:
	echo "core / Compile / logLevel := Level.Error" >> build.sbt
	echo "deploy / Compile / logLevel := Level.Error" >> build.sbt


.PHONY: sbt-build
sbt-build: version
	sbt core/makeIvyXml core/package arrowCore/makeIvyXml arrowCore/package arrowMessaging/makeIvyXml arrowMessaging/package
	mkdir -p $(PYRAPHTORY_IVYDIR)
	cp core/target/scala-2.13/ivy-$$(cat version).xml $(PYRAPHTORY_IVYDIR)/core_2.13_ivy.xml
	cp arrow-core/target/scala-2.13/ivy-$$(cat version).xml $(PYRAPHTORY_IVYDIR)/arrow-core_2.13_ivy.xml
	cp arrow-messaging/target/scala-2.13/ivy-$$(cat version).xml $(PYRAPHTORY_IVYDIR)/arrow_messaging_2.13_ivy.xml
	mkdir -p $(PYRAPHTORY_LIBDIR)
	cp core/target/scala-2.13/core_2.13-$$(cat version).jar $(PYRAPHTORY_IVYDIR)/core_2.13.jar
	cp arrow-core/target/scala-2.13/arrow-core_2.13-$$(cat version).jar $(PYRAPHTORY_IVYDIR)/arrow-core_2.13.jar
	cp arrow-messaging/target/scala-2.13/arrow-messaging_2.13-$$(cat version).jar $(PYRAPHTORY_IVYDIR)/arrow-messaging_2.13.jar


.PHONY: gh-sbt-build
gh-sbt-build: version log-level-error sbt-build


.PHONY: sbt-skip-build
sbt-skip-build: version
	ivy-clean-copy-jars

.PHONY: sbt-thin-build
sbt-thin-build: version clean sbt-build

options?=
.PHONY: python-build-options
python-build-options:


.PHONY: python-build
python-build: version python-clean-eggs python-build-options
	python -m pip install $(options) .


.PHONY: python-build-editable
python-build-editable: version python-clean-eggs python-build-options
	python -m pip install $(options) -e .


.PHONY: python-dist
python-dist: python-dist-clean sbt-build
	python -m pip install -q build
	python -m build


.PHONY: python-sdist
python-sdist: python-dist-clean
	python -m pip install -q build
	python -m build --sdist


.PHONY: python-wheel
python-wheel: python-dist-clean
	python -m pip install -q build
	python -m build --wheel


.PHONY: python-clean-eggs
python-clean-eggs:
	rm -rf python/**/*.egg-info


.PHONY: python-dist-clean
python-dist-clean: python-clean-eggs
	rm -rf dist
	rm -rf build



.PHONY: sbt-build-clean
sbt-build-clean:
	rm -rf $(PYRAPHTORY_LIBDIR)/
	rm -rf $(PYRAPHTORY_IVYDIR)/
	rm -rf $(PYRAPHTORY_JREBIN)/
	rm -rf $(PYRAPHTORY_IVYBIN)/


.PHONY: build-docs-notebooks
build-docs-notebooks:
	cd docs && pytest --nbmake --nbmake-timeout=3000 --overwrite


.PHONY: docs
docs: version
	cd docs && make html


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

.PHONY: clean-local-cluster
clean-local-cluster:
	docker-compose -f $(DOCKER_RAP)/docker-compose.yml down --remove-orphans
	rm -Rf $(DOCKER_TMP)/*

.PHONY: clean
clean: python-dist-clean sbt-build-clean
	sbt clean
	rm -Rf $(PYRAPHTORY_LIBDIR)/*



type?=patch
.PHONY: version-bump
version-bump: version
	echo "bumping $(type) version"
	pip install bump2version --quiet
	bump2version --current-version=$$(cat version) $(type)


.PHONY: release
release: version-bump
	git checkout -b v$$(cat version)
	git add version .bumpversion.cfg
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
	python -m pip install -q nbmake tox pytest-xdist build pyvis


.PHONY: setup-python
setup-python: gh-sbt-build setup-python-env python-build


.PHONY: setup-python-test
setup-python-test: python-build-options
	python -m pip install $(options) ".[test]"


.PHONY: setup-python-docs
setup-python-docs: python-build-options
	python -m pip install $(options) ".[docs]"


.PHONY: python-test
python-test:
	pytest python/build_tests
	python -X faulthandler -m pytest python/tests
	pytest --nbmake -n=auto examples
	pytest --nbmake --nbmake-timeout=3000 docs/source/Introduction
	pytest --nbmake docs/source/Install

