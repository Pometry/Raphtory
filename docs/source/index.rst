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
out our latest talk on Raphtory at `NetSciX <https://www.youtube.com/watch?v=QxhrONca4FE>`_ !

Once you are ready, hit next and lets get Raphtory :doc:`installed <Install/installdependencies>`.

.. toctree::
   :maxdepth: 3
   :hidden:
   :caption: Raphtory

   Install/installdependencies.md

   Ingestion/sprouter.md

   Analysis/LOTR_six_degrees.md
   Analysis/queries.md
   Analysis/analysis-explained.md


.. Querying/presto.md

.. toctree:: 
   :maxdepth: 2
   :hidden:
   :caption: Python Library

   PythonClient/setup.md
   PythonClient/tutorial_py_raphtory.md
   PythonClient/tutorial_algorithms.md
   PythonClient/RaphtoryClient.md
   PythonClient/conf.md


.. toctree:: 
   :maxdepth: 2
   :hidden:
   :caption: Deployment

   Deployment/baremetalsingle.md
   Deployment/graphvsservice.md  
   Deployment/pulsarlocal.md
   Deployment/kubernetes.md
  

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Examples Projects

   Examples/lotr.md
   Examples/gab.md
   Examples/community-of-cheese.md
   Examples/higgs-twitter.md
   Examples/twitter-social-circles.md
   Examples/facebook-social-circles.md
   Examples/enron.md
   


.. toctree::
   :hidden:
   :caption: Algorithms

   _autodoc/com/raphtory/algorithms/generic/index.rst
   _autodoc/com/raphtory/algorithms/temporal/index.rst


.. toctree::
   :hidden:
   :caption: API (Scaladoc)

   Scaladoc/index.md
