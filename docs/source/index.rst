.. raphtory documentation master file

Raphtory
==================

.. raw:: html

   <p align="center"><img src="_static/raphtory-banner.png" alt="Raphtory Banner"/></p>

raphtory is the new version of Raphtory, rethinking several aspects of the underlying graph model and algorithm API.

This is a temporal graph analytics engine, built natively in rust with full support in python!

You can create and analyse graphs as easy as this in python::

    from raphtory import Graph
    g = Graph(1)
    g.add_edge(0, "Ben", "Hamza", {"type": "friend"})
    g.add_edge(1, "Hamza", "Haaroon", {"type": "friend"})
    print(g.num_vertices())
    print(g.num_edges())


Raphtory can be installed using pip::

    pip install raphtory

We also have supporting for building from source and installing the rust version, please see :doc:`installed <Introduction/install>`.

If you want to dive headfirst into the API's you can visit the  :doc:`Python and Rust docs <api/index>`.

If you would like a brief summary of what it's used for before fully diving into the getting start guide please check out our latest talks and blogs on the `Raphtory website <https://raphtory.com>`_.


.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Quick Start (Python)

   install/python/raphtory.ipynb
   install/python/build.md
   install/python/test.md

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Quick Start (Rust)

   install/rust/install.md
   Introduction/quickstart.md
   install/rust/build.md
   install/rust/test.md


.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Introduction

   Introduction/ingestion.ipynb
   Introduction/how_does_it_work


.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: Documentation

   api/index
