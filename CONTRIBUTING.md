
# Contributing
We're happy that you're considering contributing!

To help you get started we've prepared the following guidelines.

## How Do I Contribute?
There are many ways to contribute:
- Report a bug
- Request a feature/enhancement
- Fix bugs
- Work on requested/approved features
- Refactor codebase
- Write tests
- Fix documentation

## Project Layout

- `raphtory`: Raphtory core written in rust
- `python`: Raphtory python library (also written in rust, converted to python with PyO3)

- `docs`: Raphtory documentation
- `examples`: Example raphtory projects in both python and rust
- `binder`: Binder configuration for running Raphtory notebooks in a browser
- `resource`: Sample CSV files

## Build Raphtory

Raphtory is written in Rust and Python

### Python Packages
Raphtory python comes as two packages `pyraphtory_jvm` and `pyraphtory`. 

Lets see how we can build these packages.

- Ensure python build dependencies are installed
    ```bash
    $ python -m pip install poetry
    ```

- Build packages
    ```bash
    $ make python-build
    ```

### Build Jar

- Build all jars
    ```bash
    $ cd Raphtory
    $ make sbt-build 
    ```

## Run Tests

### Python Tests

- Ensure test dependencies are installed
    ```bash
    $ python -m pip install nbmake tox pytest-xdist
    ```

- To run `pyraphtory` JVM tests
    ```bash
    $ cd python/pyraphtory_jvm && tox -p -o
    ```

- To run `pyraphtory` tests
    ```bash
    $ cd python/pyraphtory && poetry run pytest -n=auto
    ```

- To run notebook tests
    ```bash
    $ cd examples && pytest --nbmake -n=auto
    ```

- To run documentation notebook tests
    ```bash
    $ cd docs && pytest --nbmake -n=auto
    ```

### Scala Tests
Going by maven conventions, unit and integration tests are defined in `src/test` and `src/it` directories for any module, respectively.

Common test classes are defined in `testkit` module.

- To run tests across all modules
    ```bash
    sbt:Raphtory> test     # Runs all unit tests
    sbt:Raphtory> it/test  # Runs all integration tests
    ```
- To run tests for a module (for example)
    ```bash
    sbt:Raphtory> project it
    sbt:it> test     # Runs all unit tests
    sbt:it> it/test  # Runs all integration tests
    ```

## Update Docs
Raphtory documentations can be found in `docs` directory. They are built using [Sphinx](https://www.sphinx-doc.org/en/master/).

After making your changes, you're good to build them. 

- Ensure that all developement dependencies are already installed.
    ```bash
    $ pip install \
        ipykernel \
        autodoc \
        myst-parser \
        sphinx-rtd-theme \
        sphinx \
        docutils \
        sphinx-tabs \
        nbsphinx
    ```

- Build Raphtory
    ```bash
    $ cd Raphtory
    $ make sbt-build
    $ make python-build
    ```

- Build docs
    ```bash
    $ cd docs
    $ make html
    ```

- View docs
    ```bash
    $ open build/html/index.html
    ```
  
    **Note**: If you're not editing Scaladocs, you can disabled building Scaladocs to greatly speed up build times. Just set following flags to `False` in `docs/source/conf.py`:

    ```bash
    build_scaladocs = True
    build_algodocs = True
    ```

    
## Bounty Board 
Our bounty board lists algorithms and features we would like to build into Raphtory. 

You may want to start [here](https://www.raphtory.com/algorithm-bounty/). 

## Community Guidelines
This project follows [Google's Open Source Community Guidelines](https://opensource.google.com/conduct/).
