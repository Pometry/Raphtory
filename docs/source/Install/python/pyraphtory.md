# Installing PyRaphtory (python)

PyRaphtory comes in two parts, `pyraphtory_jvm` and `pyraphtory`.

**Both must be installed to use `pyraphtory`.**

* `pyraphtory_jvm` - Installs java if not present and downloads the ivy dependencies.
* `pyraphtory` - New version of the Raphtory python API.
  This contains all the functions to create a graph, run algorithms and analyse results.
  At the core of this iteration is the `com.raphtory.python.PyRaphtory` class that is
  able to start a Raphtory instance with python support or connect to an existing Raphtory Cluster.

  It is in experimental stage, so everything may change. 

## Install (from binary)

### Requirements

- python 3.9.13 
- pip

### Install 

    pip install requests pandas pemja cloudpickle parsy
    pip install -i https://test.pypi.org/simple/ pyraphtory_jvm==0.2.0a7
    pip install -i https://test.pypi.org/simple/ pyraphtory==0.2.0a7


