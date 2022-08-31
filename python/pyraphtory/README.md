# Getting started with `pyraphtory`

Pyraphtory is the new version of the Raphtory python API. It is in experimental stage, so everything might change.

## Running the PyRaphtory sample locally

At the core of this iteration is the `com.raphtory.python.PyRaphtory` class that is able to start a Raphtory instance
with python support or connect to an existing Raphtory Cluster (more on that later)

## Install

Please install via conda. Please see the [Raphtory](https://github.com/raphtory/raphtory) Github for more information.


### Run the PyRaphtory class

1. start pulsar and proxy `make local-pulsar`
2. (in a different shell) run pyraphtory

```bash
curl -o /tmp/lotr.csv https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv
# using make
make INPUT=/tmp/lotr.csv PYFILE=python/pyraphtory/sample.py BUILDER=LotrGraphBuilder pyraphtory-local
# or directly in bash
java -cp core/target/scala-2.13/*.jar com.raphtory.python.PyRaphtory --file=$(INPUT) --py=$(PYFILE) --builder=$(BUILDER) --mode=$(MODE)
```

## Connect to a running cluster (advanced)

1. Follow the `Prelude` and `Setup a python virtual environment with dependencies` from the `Running the PyRaphtory sample locally` guide.
2. Open the Makefile and inspect the `run-local-cluster` step, ensure the data is available for the spout and the python file is available for the builder
3. Open the `bin/docker/raphtory/docker-compose.yml` file and edit `RAPHTORY_SPOUT_FILE`, `PYRAPHTORY_GB_FILE`, `PYRAPHTORY_GB_CLASS` environment variables
4. run

```bash
java -cp core/target/scala-2.13/*.jar com.raphtory.python.PyRaphtory \
  --py python/pyraphtory/sample.py \
  --connect="raphtory.pulsar.admin.address=http://localhost:8080,raphtory.pulsar.broker.address=pulsar://127.0.0.1:6650,raphtory.zookeeper.address=127.0.0.1:2181"
```

## Links

PyPi https://pypi.org/project/pyraphtory/

Github https://github.com/Raphtory/Raphtory/

Website https://raphtory.github.io/

Slack https://raphtory.slack.com

Documentation https://raphtory.readthedocs.io/

Bug reports/Feature request https://github.com/raphtory/raphtory/issues
