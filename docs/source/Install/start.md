# Libraries

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

