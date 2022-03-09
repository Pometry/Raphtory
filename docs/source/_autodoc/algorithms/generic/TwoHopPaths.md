`com.raphtory.algorithms.generic.TwoHopPaths`
(com.raphtory.algorithms.generic.TwoHopPaths)=
# TwoHopPaths

{s}`TwoHopPaths()`
   : List all two-hop paths in the network

{s}`TwoHopPaths(seeds: Iterable[String])`, {s}`TwoHopPaths(seeds: String*)`
   : List all two-hop paths starting at nodes in {s}`seeds`

This algorithm will return the two hop neighbours of each node in
the graph. If the user provides input seeds, then it will only return
the two hop neighbours starting from those nodes.

```{Warning}
   As this sends alot of messages between nodes, running this for the entire
   graph with a large number of nodes may cause you to run out of memory.
   Therefore it is most optimal to run with a few select nodes at a time.
```

## Parameters

 {s}`seeds: Set[String]`
   : The set of node names to start paths from. If not specified, then this is
     run for all nodes.

## Returns

 | vertex1           | vertex2           | vertex3           |
 | ----------------- | ----------------- | ----------------- |
 | {s}`name: String` | {s}`name: String` | {s}`name: String` |

## Implementation

 1. In the first step the node messages all its neighbours, saying that it is
    asking for a two-hop analysis.

 2. The first-hop node forwards the message to all its neighbours, adding its name

 3. The second-hop node adds its name and responds to the source

 4. The source node compiles all response messages