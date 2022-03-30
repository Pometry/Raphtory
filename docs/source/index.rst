.. Raphtory documentation master file, created by
   sphinx-quickstart on Tue Feb  1 02:02:01 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Raphtory's documentation!
====================================
Raphtory is an open-source distributed real-time temporal graph analytics platform funded by `The Alan Turing Institute <https://www.turing.ac.uk/research/research-projects/raphtory-practical-system-analysis-dynamic-graphs>`_ and built by `Pometry <http://pometry.com>`_. This page documents the development of Raphtory including how the tool may be used, recent changes and improvements, the implemented algorithms, and use case examples for temporal graphs.

.. toctree::
   :maxdepth: 3
   :hidden:
   :caption: Getting Started:

   RunningRaphtory/install.md
   RunningRaphtory/sprouter.md
   RunningRaphtory/analysis-explained.md
   RunningRaphtory/analysis-qs.md
   RunningRaphtory/deployment.md

.. toctree::
   :maxdepth: 3
   :hidden:
   :caption: Examples:

   Examples/community-of-cheese.md
   Examples/gab.md

.. toctree::
   :maxdepth: 3
   :hidden:
   :caption: Temporal Algorithms:   
   
   Algorithms/tmotifcount.md
   Algorithms/mlpa.md

.. toctree::
   :maxdepth: 3
   :hidden:
   :caption: Static Algorithms:

   Algorithms/connectedComponents.md
   Algorithms/degree.md
   Algorithms/lpa.md
   Algorithms/cbod.md
   Algorithms/pageRank.md
   Algorithms/genericTaint.md
   Algorithms/triangleCount.md
   Algorithms/twoHopNeighbors.md
   Algorithms/wattsCascade.md

