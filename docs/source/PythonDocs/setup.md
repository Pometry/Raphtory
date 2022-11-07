   <p align="center"><img src="../_static/raphtory-banner.png" alt="Raphtory Banner"/></p>

# Building and Installing PyRaphtory

PyRaphtory comes in two parts, `pyraphtory_jvm` and `pyraphtory`. 

**Both must be installed to use `pyraphtory`.** 

## pyraphtory_jvm

`pyraphtory_jvm` - this will install Java (if it is not installed) and use ivy to download
the `pyraphtory` dependencies. These will all be saved in the package, so when uninstalled everything
will be deleted. 

### Install (from pip)

    pip install pyraphtory_jvm

### Build and Install (from source)

Building from source is only _recommended_ for advanced users or developers. 
For all other users please follow instructions above to install from pip. 

`pyraphtory_jvm` depends upon _ivy_ files generated from java. These ivy files 
are used to download dependencies required by the raphtory jar. This ensures 
we can keep the jar at a minimum size during deployment. 

Git clone the repository and `cd` into the folder. 
 
With make: 

    make sbt-build
    make python-build

Without make: 
    
    sbt publishLocal
	cp ~/.ivy2/local/com.raphtory/arrow-core_2.13/$$(cat version)/ivys/ivy.xml python/pyraphtory_jvm/pyraphtory_jvm/data/ivys/arrow_core_ivy.xml
	cp ~/.ivy2/local/com.raphtory/arrow-messaging_2.13/$$(cat version)/ivys/ivy.xml python/pyraphtory_jvm/pyraphtory_jvm/data/ivys/arrow_messaging_ivy.xml
	cp ~/.ivy2/local/com.raphtory/core_2.13/$$(cat version)/ivys/ivy.xml python/pyraphtory_jvm/pyraphtory_jvm/data/ivys/core_ivy.xml
	sed -i.bak '/org="com.raphtory"/d' python/pyraphtory_jvm/pyraphtory_jvm/data/ivys/arrow_core_ivy.xml
	sed -i.bak '/org="com.raphtory"/d' python/pyraphtory_jvm/pyraphtory_jvm/data/ivys/arrow_messaging_ivy.xml
	sed -i.bak '/org="com.raphtory"/d' python/pyraphtory_jvm/pyraphtory_jvm/data/ivys/core_ivy.xml
    cd python/pyraphtory_jvm/ && python setup.py sdist
	pip3 install python/pyraphtory_jvm/dist/pyraphtory_jvm-$$(cat version).tar.gz


## pyraphtory

This is the core of `pyraphtory` which contains all the functions to create a graph, 
 run algorithms and analyse results.

### Install (from pip)

    pip install pyraphtory_jvm

### Build and Install (from source)

Building from source is only _recommended_ for advanced users or developers.
For all other users please follow instructions above to install from pip.

`pyraphtory` depends on the jar files created when building raphtory. 
These are slim jars. These jars are used to launch a local raphtory instance
and interface with the local graphs. 

The jars do not come bundled with dependencies, these are
downloaded during the `pyraphtory_jvm` install. 


With make

    make sbt-build
    make python-build

Without make 

    sbt publishLocal
	mkdir -p python/pyraphtory/lib
	cp ~/.ivy2/local/com.raphtory/arrow-core_2.13/$$(cat version)/jars/arrow-core_2.13.jar python/pyraphtory/lib
	cp ~/.ivy2/local/com.raphtory/arrow-messaging_2.13/$$(cat version)/jars/arrow-messaging_2.13.jar python/pyraphtory/lib
	cp ~/.ivy2/local/com.raphtory/core_2.13/$$(cat version)/jars/core_2.13.jar python/pyraphtory/lib
    cd python/pyraphtory/ && \
		poetry build && \
		poetry install
	pip3 install python/pyraphtory/dist/pyraphtory-$$(cat version).tar.gz

# Publishing to pip

To publish `pyraphtory` or `pyraphtory_jvm` please follow the build process above.

Install [twine](https://twine.readthedocs.io/en/stable/)

**Note all uploads to twine must have a new version.**

**You cannot overwrite previous versions.**

To upload to test pypi

    cd python/pyraphtory_jvm or cd python/pyraphtory 
    twine upload -r testpypi dist/*

To upload to production pypi

    cd python/pyraphtory_jvm or cd python/pyraphtory 
    twine upload dist/* 

