## Build

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
