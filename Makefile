SHELL:=/bin/bash -euxo pipefail
DOCKER_RAP:=bin/docker/raphtory
DOCKER_TMP:=$(DOCKER_RAP)/tmp
MODE:=batch
IVY_VERSION:=2.5.1
export JAVA_HOME:= $(shell readlink -f $$(which java) | sed "s:/bin/java::")

version:
	sbt -Dsbt.supershell=false -error "exit" && \
	sbt -Dsbt.supershell=false -error "print core/version" | tr -d "[:cntrl:]"  > version

.PHONY gh-sbt-build:
gh-sbt-build: version
	echo "core / Compile / logLevel := Level.Error" >> build.sbt
	echo "deploy / Compile / logLevel := Level.Error" >> build.sbt
	sbt publishLocal
	cp /root/.ivy2/local/com.raphtory/arrow-core_2.13/$$(cat version)/ivys/ivy.xml python/pyraphtory_jvm/pyraphtory_jvm/data/ivys/arrow_core_ivy.xml
	cp /root/.ivy2/local/com.raphtory/arrow-messaging_2.13/$$(cat version)/ivys/ivy.xml python/pyraphtory_jvm/pyraphtory_jvm/data/ivys/arrow_messaging_ivy.xml
	cp /root/.ivy2/local/com.raphtory/core_2.13/$$(cat version)/ivys/ivy.xml python/pyraphtory_jvm/pyraphtory_jvm/data/ivys/core_ivy.xml
	sed -i.bak '/org="com.raphtory"/d' python/pyraphtory_jvm/pyraphtory_jvm/data/ivys/arrow_core_ivy.xml
	sed -i.bak '/org="com.raphtory"/d' python/pyraphtory_jvm/pyraphtory_jvm/data/ivys/arrow_messaging_ivy.xml
	sed -i.bak '/org="com.raphtory"/d' python/pyraphtory_jvm/pyraphtory_jvm/data/ivys/core_ivy.xml
	cd python/pyraphtory/ && mkdir -p lib
	cp /root/.ivy2/local/com.raphtory/arrow-core_2.13/$$(cat version)/jars/arrow-core_2.13.jar python/pyraphtory/lib
	cp /root/.ivy2/local/com.raphtory/arrow-messaging_2.13/$$(cat version)/jars/arrow-messaging_2.13.jar python/pyraphtory/lib
	cp /root/.ivy2/local/com.raphtory/core_2.13/$$(cat version)/jars/core_2.13.jar python/pyraphtory/lib

.PHONY sbt-build:
sbt-build: version
	rm -r -f ~/.ivy2/local/com.raphtory/arrow-core_2.13/$$(cat version)
	rm -r -f ~/.ivy2/local/com.raphtory/arrow-messaging_2.13/$$(cat version)
	rm -r -f ~/.ivy2/local/com.raphtory/core_2.13/$$(cat version)
	sbt publishLocal
	cp ~/.ivy2/local/com.raphtory/arrow-core_2.13/$$(cat version)/ivys/ivy.xml python/pyraphtory_jvm/pyraphtory_jvm/data/ivys/arrow_core_ivy.xml
	cp ~/.ivy2/local/com.raphtory/arrow-messaging_2.13/$$(cat version)/ivys/ivy.xml python/pyraphtory_jvm/pyraphtory_jvm/data/ivys/arrow_messaging_ivy.xml
	cp ~/.ivy2/local/com.raphtory/core_2.13/$$(cat version)/ivys/ivy.xml python/pyraphtory_jvm/pyraphtory_jvm/data/ivys/core_ivy.xml
	sed -i.bak '/org="com.raphtory"/d' python/pyraphtory_jvm/pyraphtory_jvm/data/ivys/arrow_core_ivy.xml
	sed -i.bak '/org="com.raphtory"/d' python/pyraphtory_jvm/pyraphtory_jvm/data/ivys/arrow_messaging_ivy.xml
	sed -i.bak '/org="com.raphtory"/d' python/pyraphtory_jvm/pyraphtory_jvm/data/ivys/core_ivy.xml
	cd python/pyraphtory/ && mkdir -p lib
	cp ~/.ivy2/local/com.raphtory/arrow-core_2.13/$$(cat version)/jars/arrow-core_2.13.jar python/pyraphtory/lib
	cp ~/.ivy2/local/com.raphtory/arrow-messaging_2.13/$$(cat version)/jars/arrow-messaging_2.13.jar python/pyraphtory/lib
	cp ~/.ivy2/local/com.raphtory/core_2.13/$$(cat version)/jars/core_2.13.jar python/pyraphtory/lib


in-docker-sbt-build:
	rm -Rf apache-ivy*
	wget https://dlcdn.apache.org//ant/ivy/$(IVY_VERSION)/apache-ivy-$(IVY_VERSION)-bin.zip
	unzip apache-ivy-$(IVY_VERSION)-bin.zip
	mkdir -p core/target/ivyjars
	rm -Rf core/target/ivyjars/*
	for ivy_xml in $$(ls python/pyraphtory_jvm/pyraphtory_jvm/data/ivys/core_**.xml) ; do \
		java -jar apache-ivy-$(IVY_VERSION)/ivy-$(IVY_VERSION).jar -ivy $$ivy_xml -confs runtime -retrieve "lib/[conf]/[artifact]-[type]-[revision].[ext]" ; \
	done
	rm -Rf apache-ivy*

.PHONY sbt-skip-build:
sbt-skip-build: version
	ivy-clean-copy-jars

.PHONY sbt-thin-build:
sbt-thin-build: version
	sbt clean compile package
	rm -rf python/pyraphtory/lib/
	mkdir -p python/pyraphtory/lib/
	cp core/target/scala-2.13/core_2.13-$$(cat version).jar python/pyraphtory/lib/
	cp arrow-core/target/scala-2.13/arrow-core_2.13-$$(cat version).jar python/pyraphtory/lib/
	cp arrow-messaging/target/scala-2.13/arrow-messaging_2.13-$$(cat version).jar python/pyraphtory/lib/


.PHONY python-build:
python-build: version sbt-build
	pip install -q poetry
	cd python/pyraphtory_jvm/ && \
	python setup.py sdist
	pip3 install python/pyraphtory_jvm/dist/pyraphtory_jvm-$$(cat version).tar.gz
	cd python/pyraphtory/ && \
		poetry build && \
		poetry install
	pip3 install python/pyraphtory/dist/pyraphtory-$$(cat version).tar.gz


.PHONY docker-it-image:
docker-it-image:
	docker build --build-arg DEP_JAR_PATH="$$(python3 -c "from pyraphtory_jvm.jre import get_local_ivy_loc; print(get_local_ivy_loc())")/compile" \
		--build-arg CORE_JAR_PATH="$$(python3 -c "import site; print(site.getsitepackages()[0] + '/lib/')")" -f Dockerfile-gh \
		-t raphtory-core-it:$$(cat version) \
		-t raphtory-core-it:latest \
		--compress

PHONY python-build-quick:
python-build-quick: version
	cd python/pyraphtory/ && \
		poetry build && \
		poetry install
	pip3 install python/pyraphtory/dist/pyraphtory-$$(cat version).tar.gz

.PHONY docs:
docs: version sbt-build python-build
	pip install -q myst-parser sphinx-rtd-theme sphinx docutils sphinx-tabs
	cd docs && make html

.PHONY pyraphtory-local:
pyraphtory-local: version
	java -cp core/target/scala-2.13/*.jar com.raphtory.python.PyRaphtory --input=$(INPUT) --py=$(PYFILE) --builder=$(BUILDER) --mode=$(MODE)

.PHONY: docker-build
docker-build: version
	docker build \
		--build-arg VERSION="$$(cat version)" \
		-t raphtory-core:$$(cat version) \
		-f Dockerfile . --compress
	docker tag raphtory-core:$$(cat version) raphtory-core:latest

.PHONY: docker-compose-up
docker-compose-up: version
	docker-compose -f docker-compose.yml up --build

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
	sbt clean

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

.PHONY: setup-python
setup-python: gh-sbt-build
	python -m pip install --upgrade pip
	python -m pip install -q poetry nbmake tox pytest-xdist
	cd python/pyraphtory_jvm && python setup.py sdist && python -m pip install dist/pyraphtory_jvm-*.tar.gz
	cd python/pyraphtory && poetry build && poetry install

.PHONY: python-test
python-test:
	cd python/pyraphtory_jvm && tox -p -o
	cd python/pyraphtory && poetry run pytest -n=auto
	cd examples && pytest --nbmake -n=auto
