.. Raphtory documentation master file, created by
   sphinx-quickstart on Tue Feb  1 02:02:01 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. raw:: html

   <p align="center"><img src="_static/raphtory-banner.png" alt="Raphtory Banner"/></p>

Documentation Overview
======================

Raphtory is an open-source platform for distributed real-time temporal graph analytics, allowing you to load and
process large dynamic datasets across time. Raphtory is funded in part by
`The Alan Turing Institute <https://www.turing.ac.uk/research/research-projects/raphtory-practical-system-analysis-dynamic-graphs>`_
and built by `Pometry <http://pometry.com>`_, The `QMUL Networks Group  <http://networks.eecs.qmul.ac.uk>`_ and a host of wonderful independent contributors.

If you would like a brief summary of what its used for before fully diving into the getting start guide please check
out our latest talks on Raphtory at `AIUK 2022 <https://www.youtube.com/watch?v=7S9Ymnih-YM&list=PLuD_SqLtxSdVEUsCYlb5XjWm9D6WuNKEz&index=8>`_ , `KGC 2022 <https://www.youtube.com/watch?v=37S4bSN5EaU>`_ and `NetSciX <https://www.youtube.com/watch?v=QxhrONca4FE>`_ !

Once you are ready, hit next and lets get Raphtory :doc:`installed <Install/start>`. Alternatively, if you want to dive headfirst into the API's you can visit the :doc:`ScalaDocs <Scaladoc/index>`.

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Install

   Install/start.md
   Install/python/install_conda.md
   Install/python/install_no_conda.md
   Install/scala/install.md
   Install/scala/compile_run_example.md

.. toctree::
   :maxdepth: 4
   :hidden:
   :caption: Run

   Ingestion/sprouter.md

   Analysis/LOTR_six_degrees.md
   Analysis/queries.md
   Analysis/analysis-explained.md

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Examples Projects

   Examples/lotr.md
   Examples/community-of-cheese.md
   Examples/nft.md
   Examples/gab.md
   Examples/higgs-twitter.md

.. toctree:: 
   :maxdepth: 2
   :hidden:
   :caption: Deployment

   Deployment/pulsarlocal.md
   Deployment/baremetalsingle.md
   Deployment/kubernetes.md
  
.. toctree::
   :hidden:
   :caption: Algorithms

   _autodoc/com/raphtory/algorithms/generic/index.rst
   _autodoc/com/raphtory/algorithms/temporal/index.rst
   _autodoc/com/raphtory/algorithms/filters/index.rst

.. toctree::
   :hidden:
   :maxdepth: 2
   :caption: API (Scala)

   Scaladoc/index.md

.. toctree::
   :hidden:
   :maxdepth: 2
   :caption: API (Python)

   PythonDocs/setup.md
   PythonDocs/algorithm.rst
   PythonDocs/builder.rst
   PythonDocs/formats.rst
   PythonDocs/graph.rst
   PythonDocs/sinks.rst
   PythonDocs/spouts.rst
   PythonDocs/vertex.rst
