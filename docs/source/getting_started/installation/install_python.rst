.. _install-python:

{{ header }}

=====================
Python
=====================

The easiest way to install raphtory is to install it
via pip, `pip install raphtory`.
This is the recommended installation method for most users.

.. _install.version:

Python version support
----------------------

Officially Python 3.10.

Installing raphtory
-------------------

Installing from PyPI
~~~~~~~~~~~~~~~~~~~~

Raphtory can be installed via pip from
`PyPI <https://pypi.org/project/raphtory>`__.

.. note::
    You must have ``pip>=23`` to install from PyPI.

::

    pip install raphtory


Installing from source
~~~~~~~~~~~~~~~~~~~~~~

Installing from source is the quickest way to:

* Try a new feature that will be shipped in the next release (that is, a feature from a pull-request that was recently merged to the main branch).
* Check whether a bug you encountered has been fixed since the last release.

Note that first uninstalling raphtory might be required to be able to install from source, as version numbers may not be up to date::

    pip uninstall raphtory -y

Requirements
------------

To install raphtory from source, you need the following:

* `git <https://git-scm.com/>`__ to clone the repository.
* `rust <https://www.rust-lang.org/>`__ to build the rust modules.
* `python <https://www.python.org/>`__ to run the setup script.
* `pip <https://pip.pypa.io/en/stable/>`__ to install the python package.
* `virtualenv` to create a virtual environment for the python package or `conda`
* `maturin <https://github.com/PyO3/maturin>`__ to build the python package.
* `requirements` listed  in the requirements.txt file.

Installing directly from github source
--------------------------------------

The following will pull the raphtory repository from git and install the python package from source.

    pip install -e 'git+https://github.com/Pometry/Raphtory.git#egg=raphtory&subdirectory=python'


Installing directly from source
-------------------------------

If you are developing raphtory and want to build & install the python package locally, you can do so with the following command:

    make build-all
    or
    cd python && maturin develop


Running the test suite
----------------------

Raphtory is equipped with an exhaustive set of unit tests.
To run it on your machine to verify that everything is working
(and that you have all of the dependencies installed), make sure you have `pytest
<https://docs.pytest.org/en/latest/>`__ >= 7.0

Test dependencies:

    python -m pip install -q pytest networkx numpy seaborn pandas nbmake pytest-xdist matplotlib

To run `raphtory` python tests:

    cd python && pytest

To run notebook tests:

    cd python/tests && pytest --nbmake --nbmake-timeout=1200 .
