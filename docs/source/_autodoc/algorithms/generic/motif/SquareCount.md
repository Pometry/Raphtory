`com.raphtory.algorithms.generic.motif.SquareCount`
(com.raphtory.algorithms.generic.motif.SquareCount)=
# SquareCount

{s}`SquareCount()`
  : Count undirected squares that a vertex is part of

 This is similar to counting triangles and especially useful for
 bipartite graphs. This implementation is based on the algorithm from
 [Towards Distributed Square Counting in Large Graphs](https://doi.org/10.1109/hpec49654.2021.9622799)

## States

 {s}`adjPlus: Array[Long]`
   : List of neighbours that have a larger degree than the current vertex or the same degree and a larger ID
     as computed by the [](com.raphtory.algorithms.generic.AdjPlus) algorithm.

 {s}`squareCount: Long`
   : Number of squares the vertex is part of

Returns

 | vertex name       | square count           |
 | ----------------- | ---------------------- |
 | {s}`name: String` | {s}`squareCount: Long` |