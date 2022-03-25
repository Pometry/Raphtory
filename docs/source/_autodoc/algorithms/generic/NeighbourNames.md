`com.raphtory.algorithms.generic.NeighbourNames`
(com.raphtory.algorithms.generic.NeighbourNames)=
# NeighbourNames

{s}`NeighbourNames()`
 : Get name of all neighbours and store the map from vertexID to name in state "neighbourNames".

 This is mainly useful as part of algorithms or chains that return neighbourhood or edge information.

## States

 {s}`neighbourNames: Map[Long, String]`
   : map of vertex ID to name for all neighbours of vertex

## Returns

 This algorithm does not return anything.

 ```{seealso}
 [](com.raphtory.algorithms.generic.EdgeList)
 [](com.raphtory.algorithms.temporal.TemporalEdgeList)
 ```