# Getting started with `pyraphtory`

Pyraphtory is the new version of the Raphtory python API. It is in experimental stage, so everything might change.

At the core of this iteration is the `com.raphtory.python.PyRaphtory` class that is able to start a Raphtory instance
with python support or connect to an existing Raphtory Cluster.

## Installation

There are two ways to install PyRaphtory.

1. Conda - Using the autoinstaller (This will install java, scala, sbt, raphtory and pyraphtory)

2. Source - Will build everything from scratch.

### Conda

TBD

### From Source

#### Requirements

- Temurin Java 11 - 11.0.16-tem
    - Please ensure `JAVA_HOME` environment variable is set and points to a JDK location
- scala 2.13
- sbt 1.5.5
- python 3.10
- make
- [poetry](https://python-poetry.org/)
- [Apache Pulsar](https://pulsar.apache.org/)

#### Installation guide

1. Clone Raphtory `git clone https://github.com/Raphtory/Raphtory.git` and `cd` into the root
2. Build Raphtory via `make sbt-build`
3. Build and install pyraphtory via `make python-build`

####  Running PyRaphtory

1. Start pulsar via `pulsar standalone`
2. (in a different shell) run pyraphtory examples 
3. TBD once examples are complete


# Developer notes

## Publishing to pypi with poetry

We must manually comment out (#) the `# include = ["lib/core-*.jar"]` line in the pyproject.toml
before  pushing to pypi, otherwise pypi will reject the library due to the jar being too large.

Note: You must change version name upon each push, to overwrites the previous version

### PyPi Production environment

#### First config poetry

    poetry config repositories.pypi https://pypi.org/

##### With dry run

`--dry-run` will not publish the package, it will just build the package locally

    poetry publish -r pypi -u USERNAME -p PASSWORD --dry-run

##### Without dry run

    poetry publish -r pypi -u USERNAME -p PASSWORD

### PyPi Test Environment

#### First config poetry

    poetry config repositories.testpypi https://test.pypi.org/legacy/

##### With dry run

`--dry-run` will not publish the package, it will just build the package locally

    poetry publish -r testpypi -u USERNAME -p PASSWORD --dry-run

##### Without dry run

    poetry publish -r testpypi -u USERNAME -p PASSWORD

## Links

- PyPi https://pypi.org/project/pyraphtory/
- Github https://github.com/Raphtory/Raphtory/
- Website https://raphtory.github.io/
- Slack https://raphtory.slack.com
- Documentation https://raphtory.readthedocs.io/
- Bug reports/Feature request https://github.com/raphtory/raphtory/issues