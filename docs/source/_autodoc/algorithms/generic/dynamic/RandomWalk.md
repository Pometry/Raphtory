`com.raphtory.algorithms.generic.dynamic.RandomWalk`
(com.raphtory.algorithms.generic.dynamic.RandomWalk)=
# RandomWalk

{s}`RandomWalk(walkLength: Int = 10, numWalks: Int = 1, seed: Long = -1)`
   : Implements random walks on unweighted graph

 This algorithm starts `numWalks` unbiased random walks from each node
 and terminates them after the walks have reached `walkLength` nodes. The network is treated as directed and if a
 vertex has no outgoing edges, the walk remains at this vertex for the remaining steps.

## Parameters

 {s}`walkLength: Int = 10`
   : maximum length of generated walks

 {s}`numWalks: Int = 1`
   : number of walks to start for each node

 {s}`seed: Long`
   : seed for the random number generator

```{note}
Currently, results are non-deterministic even with fixed seed, likely due to non-deterministic message order.
```

## States

 {s}`walks: Array[ArrayBuffer[String]]`: List of nodes for each random walk started from this vertex

## Returns

| vertex 1          | vertex 2          | ... | vertex `walkLength` |
| ----------------- | ----------------- | --- | ------------------- |
| {s}`name: String` | {s}`name: String` | ... | {s}`name: String`   |

 Each row of the table corresponds to a single random walk and columns correspond to the vertex at a given step