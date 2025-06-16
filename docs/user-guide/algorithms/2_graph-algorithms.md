# Graph wide algorithms

The following examples cover three `graphwide` algorithms:

* [Graph Density](https://en.wikipedia.org/wiki/Dense_graph) - which represents the ratio between the edges present in a graph and the maximum number of edges that the graph could contain.
* [Clustering coefficient](https://en.wikipedia.org/wiki/Clustering_coefficient) - which is a measure of the degree to which nodes in a graph tend to cluster together e.g. how many of your friends are also friends.
* [Reciprocity](https://en.wikipedia.org/wiki/Reciprocity_(network_science)) - which is a measure of the likelihood of nodes in a directed network to be mutually connected e.g. if you follow someone on twitter, whats the change of them following you back.

To run an algorithm you simply need to import the algorithm package, choose an algorithm to run, and give it your graph.

{{code_block('getting-started/algorithms','global',['Graph'])}}

!!! Output

    ```python exec="on" result="text" session="algorithms"
    --8<-- "python/getting-started/algorithms.py:global"
    ```
