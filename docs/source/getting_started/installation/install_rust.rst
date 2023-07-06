.. _install-rust:

{{ header }}

#########################################
Rust (Cargo, Source, Testing, Package)
#########################################

The easiest way to install raphtory is to install it
via cargo, `cargo add raphtory`.
This is the recommended installation method for most users.

.. _install.version-rust:

Version
***************

Officially Rust 1.67.1

Installing raphtory
********************

Cargo
===========

Raphtory can be installed via pip from
`Cargo <https://crates.io/crates/raphtory>`__.

.. note::
    You must have ``rust>=1.67.1`` to install from cargo.

::

    cargo add raphtory


Source
===========

Installing from source is the quickest way to:

* Try a new feature that will be shipped in the next release (that is, a feature from a pull-request that was recently merged to the main branch).
* Check whether a bug you encountered has been fixed since the last release.

.. note::
     Uninstalling raphtory might be required to be able to install from source, as version numbers may not be up to date

::

    cargo remove raphtory

Requirements
--------------

To install raphtory from source, you need the following:

* `git <https://git-scm.com/>`__ to clone the repository.
* `rust <https://www.rust-lang.org/>`__ to build the rust modules.
* `make <https://www.gnu.org/software/make/>`__ to run the build script.

Build commands
-------------------------------

Building the rust core is done using cargo. The following command will build the core.

::

    make rust-build

or

::

    cargo build


Importing into a rust project
************************************************

To use the raphtory core in a rust project, add the following to your Cargo.toml file:

.. note::
    The path should be the path to the raphtory directory.

    You must change ``X.Y.Z`` with the latest released version i.e. ``0.4.1``


::

    [dependencies]

    raphtory = {path = "../raphtory", version = "X.Y.Z" }


or

::

    [dependencies]

    raphtory = "X.Y.Z"


Running the tests
************************************************

Raphtory is equipped with an exhaustive set of unit tests.
To run it on your machine to verify that everything is working
(and that you have all of the dependencies installed)


::

    cargo test

