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
    raphtory = {path = "../raphtory", version = "0.3.0" }
     
    or 

    [dependencies]
    raphtory = "0.3.0"


## Run Tests

### Python Tests

- Ensure test dependencies are installed
    ```bash
    $ python -m pip install -q pytest networkx numpy seaborn pandas nbmake pytest-xdist matplotlib
    ```

- To run `raphtory` python tests
    ```bash
    $ cd python && pytest
    ```

- To run `raphtory` rust tests
    ```bash
    $ cargo test
    ```

- To run notebook tests
    ```bash
    $ cd python/tests && pytest --nbmake --nbmake-timeout=1200 .
    ```

- To run documentation notebook tests
    ```bash
    $ cd docs && pytest --nbmake -n=auto
    ```
- To run documentation tests
    ```bash
    $ sudo apt update.md && sudo apt install -y pandoc make python3-sphinx
    $ cd docs && pip install -q -r requirements.txt && make html
    ```

## Update Docs
Raphtory documentations can be found in `docs` directory. 
They are built using [Sphinx](https://www.sphinx-doc.org/en/master/) and hosted by readthedocs. 

After making your changes, you're good to build them. 

- Ensure that all developement dependencies are already installed.
    ```bash
    $ cd docs && pip install -q -r requirements.txt
    ```

- Build docs
    ```bash
    $ cd docs && make html
    ```

- View docs
    ```bash
    $ open build/html/index.html
    ```
    
## Bounty Board 
Our bounty board lists algorithms and features we would like to build into Raphtory. 

You may want to start [here](https://github.com/Raphtory/Raphtory/discussions/categories/bounty-board/). 

## Community Guidelines
This project follows [Google's Open Source Community Guidelines](https://opensource.google.com/conduct/).
