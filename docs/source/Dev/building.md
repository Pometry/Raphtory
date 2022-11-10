## Build & Install (from source, python & scala)

Building from source is only _recommended_ for advanced users or developers.
For all other users please follow instructions above to install from pip.

### Requirements

- Temurin Java 11 - 11.0.16-tem
    - Please ensure `JAVA_HOME` environment variable is set and points to a JDK location
    - [Click here for the guide to install java and sbt](/Install/scala/install_java.md)
- scala 2.13
- sbt
- python 3.9.13
- make
- [poetry](https://python-poetry.org/)


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