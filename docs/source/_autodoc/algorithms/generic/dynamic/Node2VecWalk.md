`com.raphtory.algorithms.generic.dynamic.Node2VecWalk`
(com.raphtory.algorithms.generic.dynamic.Node2VecWalk)=
# Node2VecWalk

{s}`Node2VecWalk(walkLength:Int = 10, p: Double = 1.0, q: Double = 1.0)`
 : Node2Vec biased random walk on directed, unweighted graph

Node2Vec is used to construct feature vectors to represent vertices or edges in a graph such that classical machine
learning algorithms can be applied to the resulting vector representations for network analysis tasks,
such as node classification and link prediction. This algorithm implements the Node2Vec biased 2nd-order random
walk model to sample neighbors of every vertex in a graph. This random walk model interpolates between breadth-first
and depth-first search in the graph in a flexible manner so that local and global structures of the vertex
neighborhood can both be captured. In this way, it is capable of supporting different varieties of graphs
and analysis tasks well.This implementation is based on [Fast-Node2Vec](https://arxiv.org/pdf/1805.00280.pdf)
and computes transition probabilities on the fly rather than precomputing all probabilities in advance.

```{note}
This implementation currently does not support edge weights. This algorithm also only implements the random walk
step of Node2Vec. For a full implementation of Node2Vec, the resulting output should be fed into a skip-gram model.[^node2vec]
```

```{note}
If the walk reaches a vertex with out-degree 0, it will remain there until {s}`walkLength` is reached.
```

## Parameters

 {s}`walkLength: Int = 10`
   : Lengths of the generated random walks

 {s}`p: Double = 1.0`
   : bias parameter $p$

 {s}`q: Double = 1.0`
   : bias parameter $q$

## States

 {s}`walk: ArrayBuffer[String]`
   : List of vertices visited by the random walk starting from this vertex

## Returns

| vertex 1          | vertex 2          | ... | vertex `walkLength` |
| ----------------- | ----------------- | --- | ------------------- |
| {s}`name: String` | {s}`name: String` | ... | {s}`name: String`   |

 Each row of the table corresponds to a single random walk and columns correspond to the vertex at a given step.
 The algorithm starts one random walk from each vertex in the graph.

[^node2vec]: [node2vec: Scalable Feature Learning for Networks](https://arxiv.org/abs/1607.00653)