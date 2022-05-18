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
out our latest talks on Raphtory at `AIUK 20222 <https://www.youtube.com/watch?v=7S9Ymnih-YM&list=PLuD_SqLtxSdVEUsCYlb5XjWm9D6WuNKEz&index=8>`_ , `KGC 2022 <https://www.youtube.com/watch?v=37S4bSN5EaU>`_ and `NetSciX <https://www.youtube.com/watch?v=QxhrONca4FE>`_ !

Once you are ready, hit next and lets get Raphtory :doc:`installed <Install/installdependencies>`. Alternatively, if you want to dive headfirst into the internals you visit the :doc:`ScalaDocs <Scaladoc/index.md>`.

.. toctree::
   :maxdepth: 3
   :hidden:
   :caption: Raphtory

   Install/installdependencies.md

   Ingestion/sprouter.md

   Analysis/LOTR_six_degrees.md
   Analysis/queries.md
   Analysis/analysis-explained.md

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


.. toctree::
   :hidden:
   :caption: API (Scaladoc)

   Scaladoc/index.md   


.. toctree:: 
   :maxdepth: 2
   :hidden:
   :caption: Python Library (Alpha)

   PythonClient/setup.md
   PythonClient/tutorial_pulsar.md
   PythonClient/RaphtoryClient.md
   PythonClient/tutorial_algorithms.md
   PythonClient/conf.md
   


.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Examples Projects

   Examples/lotr.md
   Examples/community-of-cheese.md
   Examples/gab.md
   Examples/higgs-twitter.md
   Examples/twitter-social-circles.md
   Examples/facebook-social-circles.md
   Examples/enron.md

