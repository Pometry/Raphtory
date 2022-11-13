.. Raphtory documentation master file, created by
   sphinx-quickstart on Tue Feb  1 02:02:01 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. raw:: html

   <p align="center"><img src="_static/raphtory-banner.png" alt="Raphtory Banner"/></p>

Documentation Overview
======================

Raphtory is an open-source platform for distributed real-time temporal graph analytics, allowing you to load and process large dynamic datasets across time. Raphtory is funded in part by `The Alan Turing Institute <https://www.turing.ac.uk/research/research-projects/raphtory-practical-system-analysis-dynamic-graphs>`_ and built by `Pometry <http://pometry.com>`_, The `QMUL Networks Group  <http://networks.eecs.qmul.ac.uk>`_ and a host of wonderful independent contributors.

If you would like a brief summary of what it's used for before fully diving into the getting start guide please check out our latest talks and blogs on the `Raphtory website <https://raphtory.com>`_.

Once you are ready, hit next and lets get Raphtory :doc:`installed <Install/python/pyraphtory>`. Alternatively, if you want to dive headfirst into the API's you can visit the :doc:`PythonDocs <PythonDocs/index>` or :doc:`ScalaDocs <Scaladoc/index>`.

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: PyRaphtory Introduction

   Install/python/pyraphtory.ipynb
   Ingestion/sprouter.md
   Analysis/queries.md
     
.. toctree::
   :hidden:
   :caption: Inbuilt Algorithms

   _autodoc/com/raphtory/algorithms/generic/index.rst
   _autodoc/com/raphtory/algorithms/temporal/index.rst
   _autodoc/com/raphtory/algorithms/filters/index.rst

.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: Writing your own Algorithm

   Analysis/LOTR_six_degrees.md
   Analysis/analysis-explained.md

.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: PyRaphtory API

   PythonDocs/index.md

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Example Projects (Scala & Python)

   Examples/lotr.md
   Examples/nft.md
   Examples/higgs-twitter.md

.. toctree:: 
   :maxdepth: 2
   :hidden:
   :caption: Deployment

   Deployment/baremetalsingle.md
   Deployment/kubernetes.md

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Contributing to Raphtory

   Install/scala/raphtory.md
   Install/scala/install_java.md
   Install/scala/compile_run_example.md
   Dev/building.md
   Dev/publishing.md

   .. toctree::
   :hidden:
   :maxdepth: 2
   :caption: API (Scala)

   Scaladoc/index.md


   ..  
      PythonDocs/algorithm.rst
      PythonDocs/builder.rst
      PythonDocs/formats.rst
      PythonDocs/graph.rst
      PythonDocs/sinks.rst
      PythonDocs/spouts.rst
      PythonDocs/vertex.rst