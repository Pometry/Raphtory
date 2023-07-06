.. _overview:

{{ header }}

******************
Project overview
******************

The Raphtory model consists of a base temporal graph.
It maintains a chronological log of all changes to a graph's structure and property values over time.
You can create a graph using simple functions to add/remove vertices and edges at different time
points, as well as update their properties.
Alternatively, you can generate a graph from common data sources/formats using built-in loaders for
common data sources/formats.


Once you have a graph, you can generate "graph views" to observe the underlying graph with
specific structural or temporal constraints.
Programmatically, you can create graph views over desired time ranges (windows),
over sets of nodes that meet user-defined criteria (subgraphs), or over a subset of layers in multilayered graphs.


These views support event durations and provide different deletion semantics.
To save memory, graph views are materialized only when accessed.
This allows you to maintain multiple perspectives of your graph simultaneously.
You can explore and compare these perspectives using graph algorithms and metrics.
Additionally, Raphtory offers extensions for generating null models and exporting views
to other graph libraries like NetworkX.

Raphtory includes fast and scalable implementations of algorithms for temporal network mining,
including temporal motifs and temporal reachability.
It exposes an internal API for implementing algorithms in Rust and surfaces them in Python.
Installing Raphtory is straightforward using standard Python and Rust package managers.
Once installed, you can integrate it into your analysis pipeline or run it as a standalone GraphQL service.


Core Functions
--------------------

|1a| |1b|

.. |1a| image:: /_static/core_functions_1_a.png
   :width: 45%
   :alt: Python code showing how to add edges with properties using Raphtory

.. |1b| image:: /_static/core_functions_1_b.png
   :width: 45%
   :alt: A timeline gantt chart showing interactions of emails between users

1. Edges are dynamical entities connecting pairs of nodes, and can be added and removed at any time


|2a| |2b|

.. |2a| image:: /_static/core_functions_2_a.png
   :width: 45%
   :alt: Python code showing how to extract and run algorithms on graph views in Raphtory

.. |2b| image:: /_static/core_functions_2_b.png
   :width: 45%
   :alt: A line graph showing the PageRank


2. Graph views can be generated at any given time with any resolution and on selected layers, to run standard network algorithms.


|3a| |3b|

.. |3a| image:: /_static/core_functions_3_a.png
   :width: 45%

.. |3b| image:: /_static/core_functions_3_b.png
   :width: 45%


3. Rapid implementations of algorithms specifically designed for temporal networks



Projects using Raphtory
-----------------------

Raphtory has proved an invaluable resource in industrial and academic projects,
for instance:

* To characterise the time evolution of the `fringe social network Gab <https://dl.acm.org/doi/10.1145/3479591>`__
* Transactions of users of a dark web marketplace Alphabay using `temporal motifs <https://dl.acm.org/doi/abs/10.1145/3018661.3018731>`__
* Anomalous patterns of `activity in NFT trades <https://arxiv.org/abs/2303.11181>`__

The library has recently been significantly rewritten, and we expect that with its new functionalities,
efficiency and ease of use, it will become an essential part of the network science community.


Getting support
---------------

The first stop for raphtory issues and ideas is the `GitHub Issue Tracker
<https://github.com/pometry/raphtory/issues>`__. If you have a general question,
raphtory community experts can answer through `Slack
<https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA>`__.


