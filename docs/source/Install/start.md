# Installation

Raphtory has support for graph analytics in both Scala and Python.
The core analytics platform is written in [Scala](https://www.scala-lang.org).

As such both Java and Scala are required to run either `Raphtory` or `PyRaphtory`.

* `Raphtory` - Scala library
* `PyRaphtory` - Python library
  * `pyraphtory_jvm` - Installs java if not present and downloads the ivy dependencies. 
  * `pyraphtory` - New version of the Raphtory python API.
    This contains all the functions to create a graph, run algorithms and analyse results.
    At the core of this iteration is the `com.raphtory.python.PyRaphtory` class that is
    able to start a Raphtory instance with python support or connect to an existing Raphtory Cluster.

    It is in experimental stage, so everything may change. 

Both libraries can be installed from their binaries or from source. 
**We recommend installing from the binaries.**

## Install (from binary)

### Requirements

````{tabs}

```{group-tab} Python

- python 3.9.13 
- pip

```

```{group-tab} scala

- Temurin Java 11 - 11.0.16-tem
  - Please ensure `JAVA_HOME` environment variable is set and points to a JDK location
- sbt

[Click here for the guide to install java and sbt](../Install/install_java.md)

```
````

### Guide

````{tabs}

```{code-tab} python

pip install requests pandas pemja cloudpickle parsy
pip install -i https://test.pypi.org/simple/ pyraphtory_jvm==0.2.0a7
pip install -i https://test.pypi.org/simple/ pyraphtory==0.2.0a7

```

```{group-tab} scala
Download the jar file below and add as a library into your code editor  

https://github.com/Raphtory/Raphtory/releases/download/v0.2.0a7/core-assembly-0.2.0a7.jar 

```
````

## Build & Install (from source)

Building from source is only _recommended_ for advanced users or developers.
For all other users please follow instructions above to install from pip.

### Requirements

- Temurin Java 11 - 11.0.16-tem
  - Please ensure `JAVA_HOME` environment variable is set and points to a JDK location
- scala 2.13
- sbt
- python 3.9.13
- make
- [poetry](https://python-poetry.org/)

[Click here for the guide to install java and sbt](../Install/install_java.md)


### Guide

````{tabs}

```{group-tab} Python

PyRaphtory comes in two parts, `pyraphtory_jvm` and `pyraphtory`. 

**Both must be installed to use `pyraphtory`.**

Here we will build both `pyraphtory_jvm` and `pyraphtory`

`pyraphtory_jvm` depends upon _ivy_ files generated from java. These ivy files 
are used to download dependencies required by the raphtory jar. This ensures 
we can keep the jar at a minimum size during deployment. 

`pyraphtory` depends on the jar files created when building raphtory. 
These are slim jars. These jars are used to launch a local raphtory instance
and interface with the local graphs. 

The command below will build and install both `pyraphtory_jvm` and `pyraphtory`.

    git clone https://github.com/Raphtory/Raphtory/tree/development && cd Raphtory
    git checkout development 
    make sbt-build
    make python-build

```

```{group-tab} scala

Let's clone the Raphtory repository using Git and checkout into the latest development version

    git clone https://github.com/Raphtory/Raphtory/tree/development && cd Raphtory
    git checkout development 
    make sbt-build

It should end with a success

    [success] Total time: 103 s (01:43), completed 16 Sep 2022, 15:36:37

```
````
Everything should now be installed and ready for us to get your first Raphtory Job underway!

