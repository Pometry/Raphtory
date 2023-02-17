.. DocBrown documentation master file

.. raw:: html

   <p align="center"><img src="_static/raphtory-banner.png" alt="Raphtory Banner"/></p>

Documentation Overview
======================

DocBrown is the Rust prototype for the next version of Raphtory, rethinking several aspects of the underlying graph model and algorithm API.

Please checkout the issues for the core features to be included in this version, along with their proposed semantics.

Below is a diagram of how DocBrown works:

.. raw:: html

   <p align="center">
   <img src="https://user-images.githubusercontent.com/25484244/218711926-944092df-5015-4c7e-8162-34ee044999f4.svg" height=500 alt="Raphtory-DocBrown-Diagram"/>
   </p>

GraphDB is the overarching manager for the graph. A GraphDB instance can have N number of shards. These shards (also called TemporalGraphParts) store fragments of a graph. If you have used the Scala/Python version of Raphtory before, the notion of shards is similar to partitions in Raphtory, where parts of a graph are stored into partitions. When an edge or node is added to the graph in Doc Brown, GraphDB will search for an appropriate place inside a shard to place these. In this diagram, there are 4 shards (or partitions) labelled as S1, S2, S3, S4. Altogether they make up the entire temporal graph, hence the name "TemporalGraphPart" for each shard.

Shards are used for performance and distribution reasons. Having multiple shards running in parallel speeds up things a lot in Raphtory. In a matter of seconds, you are able to see your results from your temporal graph analysis. Furthermore, you can run your analysis across multiple machines (e.g. one shard per machine).

Note: DocBrown is in heavy active development.  


If you would like a brief summary of what it's used for before fully diving into the getting start guide please check out our latest talks and blogs on the `Raphtory website <https://raphtory.com>`_.


Alternatively, hit next and lets get Raphtory :doc:`installed <Introduction/install>`. If you want to dive headfirst into the API's you can visit the  :doc:`RustDocs <Introduction/apidocs>`.


.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Introduction

   Introduction/install.md
   Introduction/apidocs.md
   Introduction/quickstart.md
