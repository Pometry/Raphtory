How does raphtory work?
======================

   <p align="center">
   <img src="https://user-images.githubusercontent.com/25484244/218711926-944092df-5015-4c7e-8162-34ee044999f4.svg" height=500 alt="Raphtory-raphtory-Diagram"/>
   </p>

GraphDB is the overarching manager for the graph. A GraphDB instance can have N number of shards. These shards (also called TemporalGraphParts) store fragments of a graph. If you have used the Scala/Python version of Raphtory before, the notion of shards is similar to partitions in Raphtory, where parts of a graph are stored into partitions. When an edge or node is added to the graph in Doc Brown, GraphDB will search for an appropriate place inside a shard to place these. In this diagram, there are 4 shards (or partitions) labelled as S1, S2, S3, S4. Altogether they make up the entire temporal graph, hence the name "TemporalGraphPart" for each shard.

Shards are used for performance and distribution reasons. Having multiple shards running in parallel speeds up things a lot in Raphtory. In a matter of seconds, you are able to see your results from your temporal graph analysis. Furthermore, you can run your analysis across multiple machines (e.g. one shard per machine).

Note: raphtory is in heavy active development.
