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

- `raphtory`: Raphtory Core written in rust
- `python`: Raphtory python library (also written in rust, converted to python with PyO3)

- `docs`: Documentation (built and hosted using sphinx and readthedocs)
- `examples`: Example raphtory projects in both python and rust
- `resource`: Sample CSV files

## Build Raphtory

### Python package  

#### Install directly from source 

The following will pull the raphtory repository from git and install the python package from source.
Note: This requires that you have the rust toolchain installed and python3.10

    pip install -e 'git+https://github.com/Raphtory/Raphtory.git#egg=raphtory&subdirectory=python'

#### Build during development 

If you are developing raphtory and want to build & install the python package locally, you can do so with the following command:
Note: This requires that you have installed python3.10 in either a virtual environment or conda. and have installed the 
`maturin` python package 

    make build-all
    or 
    cd python && maturin develop

#### Import into your local environment

Now simply run below to use the package:

    import raphtory   

### Rust core

#### Build from source

Building the rust core is done using cargo. The following command will build the core.
Note: This requires that you have the rust toolchain installed.

    make rust-build
    or
    cargo build

#### Import the raphtory package into a rust project 

To use the raphtory core in a rust project, add the following to your Cargo.toml file:
Note: The path should be the path to the raphtory directory


    [dependencies]
    raphtory = {path = "../raphtory", version = "0.0.11" }
     
    or 

    [dependencies]
    raphtory = "0.0.11"


## Run Tests

### Python Tests

- Ensure test dependencies are installed
    ```bash
    $ python -m pip install nbmake tox pytest-xdist
    ```

- To run `raphtory` python tests
    ```bash
    $ cd python/pyraphtory_jvm && tox -p -o
    ```

- To run `raphtory` rust
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
