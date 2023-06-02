{{ header }}

.. _contributing:

=============
Contributing
=============

We're happy that you're considering contributing!

To help you get started we've prepared the following guidelines.

How Do I Contribute?
~~~~~~~~~~~~~~~~~~~~

There are many ways to contribute:

- Report a bug
- Request a feature/enhancement
- Fix bugs
- Work on requested/approved features
- Refactor codebase
- Write tests
- Fix documentation

Project Layout
~~~~~~~~~~~~~~~

- `raphtory`: Raphtory Core written in rust
- `py-raphtory`: Raphtory python library (written in rust, converted to python with PyO3)
- `python`: Raphtory python helper scripts
- `benchmark`: Benchmarking scripts used to compare Raphtory to other platforms
- `raphtory-benchmark`: Benchmarking scripts run in the CI/CD pipeline
- `raphtory-graphql`: GraphQL server for raphtory
- `raphtory-io`: IO module for raphtory
- `js-raphtory`: Raphtory javascript library


- `docs`: Documentation (built and hosted using sphinx and readthedocs)
- `examples`: Example raphtory projects in both python and rust
- `resource`: Sample CSV files


Documentation
==============

Raphtory documentations can be found in `docs` directory.
They are built using `Sphinx <https://www.sphinx-doc.org/en/master/>`__ and hosted by readthedocs.

After making your changes, you're good to build them.

- Ensure that all development dependencies are already installed.
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

Community Guidelines
=====================

This project follows `Google's Open Source Community Guidelines <https://opensource.google.com/conduct/>`__.


License
========

Raphtory it licensed under  `GNU General Public License v3.0`.

This docs page is licensed under `BSD 3-Clause License`.