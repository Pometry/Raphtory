   <p align="center"><img src="../_static/raphtory-banner.png" alt="Raphtory Banner"/></p>

# (Developer notes) Publishing to pypi

## Pyraphtory_jvm  - Publishing to pypi with twine 

`pyraphtory_jvm` is published to pypi using twine. 

First follow the [build process](start.md) to create the tar files. 

### Requirements

- python 3.9.13
- pip
- [twine](https://twine.readthedocs.io/en/stable/)

**You must change version name upon each upload, to overwrite the previous version.**

**If you do not, your release will be rejected.**

### pypi test environment

    cd python/pyraphtory_jvm
    twine upload -r testpypi dist/*

### pypi production environment

    cd python/pyraphtory_jvm 
    twine upload dist/* 

## Pyraphtory - Publishing to pypi with poetry

`pyraphtory` is built and packaged using poetry. 

### Requirements

- python 3.9.13
- pip
- [poetry](https://python-poetry.org/)

**You must change version name upon each upload, to overwrite the previous version.** 

**If you do not, your release will be rejected.** 


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




