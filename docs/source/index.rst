.. Raphtory documentation master file, created by
   sphinx-quickstart on Tue Feb  1 02:02:01 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. warning::
   This is the documentation for Raphtory 0.5.0-alpha. In this version Raphtory has been completely rebuilt to run on top of `Apache Pulsar <https://pulsar.apache.org>`_, moving away from Akka. This has fixed a number of issues faced in prior versions, notably around message back pressure, and introduces many exciting features which are discussed throughout these pages. 
   
   0.5.0 is very usable, however, it is still an alpha version being actively developed. If you notice any issues or have feature suggestions please raise an issue or come and discuss it with us on `Slack <https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA>`_. 


.. raw:: html

   <p align="center"><img src="_static/raphtory-banner.png" alt="Raphtory Banner"/></p>

Documentation Overview
======================

Raphtory is an open-source platform for distributed real-time temporal graph analytics, allowing you to load and
process large dynamic datasets across time. Raphtory is funded in part by
`The Alan Turing Institute <https://www.turing.ac.uk/research/research-projects/raphtory-practical-system-analysis-dynamic-graphs>`_
and built by `Pometry <http://pometry.com>`_, The `QMUL Networks Group  <http://networks.eecs.qmul.ac.uk>`_ and a host of wonderful independent contributors.

If you would like a brief summary of what its used for before fully diving into the getting start guide please check
out this `article <https://www.turing.ac.uk/blog/just-add-time-dizzying-potential-dynamic-graphs>`_
from the Alan Turing Institute first! Once you are ready, hit next and lets get Raphtory :doc:`installed <Install/installdependencies>`.

.. toctree::
   :maxdepth: 3
   :hidden:
   :caption: Raphtory

   Install/installdependencies.md

   Ingestion/sprouter.md

   Analysis/LOTR_six_degrees.md
   Analysis/queries.md
   Analysis/analysis-explained.md

   Querying/presto.md

.. toctree:: 
   :maxdepth: 2
   :hidden:
   :caption: Python Library

   PythonClient/setup.md
   PythonClient/tutorial.md
   PythonClient/RaphtoryClient.md
   PythonClient/conf.md
   PythonClient/algorithms.md

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

   Examples/community-of-cheese.md
   Examples/gab.md
   Examples/higgs-twitter.md
   Examples/twitter-social-circles.md
   Examples/facebook-social-circles.md
   Examples/enron.md
   Examples/lotr.md


.. toctree::
   :hidden:
   :caption: API

   _autodoc/com/raphtory/index.rst


