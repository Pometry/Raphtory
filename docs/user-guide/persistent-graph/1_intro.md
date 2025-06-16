# Graph and persistent graph

Up to now, we have made an implicit assumption that a temporal graph is made up of events (messages, citations, transactions) instantiated from one node to another that can be thought of as instantaneous. In the temporal graph literature, such representations of graphs are known as *link streams*. This representation can also capture temporal graphs that can be considered a sequence of static network snapshots by giving edges integer timestamps `1, 2, 3, ...` depending on the snapshot(s) in which they are found.

However, there is another family of temporal graphs which don't fit into this format. What about instead of an instantaneous event, edges could be present for a defined amount of time? Such occasions might include proximity networks where an edge between two individuals is continuously present whenever they are close together, or a 'following' network where edges are established and later potentially removed. 

To enable these types of interactions to be represented, we provide an additional graph representation where edges can be added, removed and added back again called the *PersistentGraph*. 

The example below shows how to create and manipulate a *PersistentGraph* in Raphtory.

{{code_block('getting-started/persistent-graph','simple_graph',['Graph'])}}
!!! Output

    ```python exec="on" result="text" session="getting-started/persistent-graph"
    --8<-- "python/getting-started/persistent-graph.py:simple_graph"
    ```

Here we have a graph with two edges: one connecting Alice and Bob, and one connecting Bob and Charlie, and three _exploded edges_, one for each activation of Alice and Bob's edge and the activation of Bob and Charlie's edge. If an edge is not explicitly deleted, it is assumed to last forever (or at least until an integer max value).

Over the next few pages, we will explore how the persistent graph works to understand its behaviour and semantics and how it can unlock some interesting analysis.