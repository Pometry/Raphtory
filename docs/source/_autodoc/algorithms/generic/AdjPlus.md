`com.raphtory.algorithms.generic.AdjPlus`
(com.raphtory.algorithms.generic.AdjPlus)=
# AdjPlus

{s}`AdjPlus()`
: AdjPlus transform of the graph

 This transforms the graph using the AdjPlus projection, which treats the input graph as undirected and returns a
 directed graph where all edges point from low to high degree.
 For each vertex, the algorithm finds the set of neighbours that have a larger degree than the current vertex
 or the same degree and a larger ID and store it as state "adjPlus". This algorithm treats the network as undirected.
 Further, the vertex IDs in "adjPlus" are ordered by increasing degree. This projection is particularly useful to
 make certain motif-counting algorithms more efficient.

## States

 {s}`adjPlus: Array[Long]`
 : List of neighbour IDs that have a larger degree than the current vertex
   or the same degree and a larger ID, ordered by increasing degree

## Returns
 edge list for the AdjPlus projection

 | source name          | destination name     |
 | -------------------- | -------------------- |
 | {s}`srcName: String` | {s}`dstName: String` |

```{seealso}
[](com.raphtory.algorithms.generic.motif.SquareCount)
```