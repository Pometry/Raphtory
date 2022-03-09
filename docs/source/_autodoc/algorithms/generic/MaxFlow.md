`com.raphtory.algorithms.generic.MaxFlow`
(com.raphtory.algorithms.generic.MaxFlow)=
# MaxFlow
{s}`MaxFlow[T](source: String, target: String, capacityLabel: String = "weight", maxIterations: Int = Int.MaxValue)`
  : Finds the maximum flow (equivalently, the minimum cut) between a source and a target vertex

  Implements the parallel push-relabel max-flow algorithm of Goldberg and Tarjan. [^ref]

## Parameters
 {s}`T: Numeric`
   : Type of edge weight attribute (needs to be specified as not possible to infer automatically)

 {s}`source: String`
   : name of source vertex

 {s}`target: String`
   : name of target vertex

 {s}`capacityLabel: String = "weight"`
   : Edge attribute key to use for computing capacities. Capacities are computed as the sum of weights over occurrences
     of the edge. If the edge property does not exist, weight is computed as the edge count.

  {s}`maxIterations: Int = Int.MaxValue`
   : terminate the algorithm if it has not converged after {s}`maxIterations` pulses (note that the flow will be
     incorrect if this happens)

 ## States

 {s}`flow: mutable.Map[Long, T]`
   : Map of {s}`targetID -> flow`  for outflow at vertex (negative values are backflow used by the algorithm)

 {s}`excess: T`
   : excess flow at vertex (should be 0 except for source and target after algorithm converged)

 {s}`distanceLabel: Int`
   : vertex label used by the algorithm to decide where to push flow

 {s}`neighbourLabels: mutable.Map[Long, Int]`
   : Map of {s}`targetID -> label` containing the distance labels for the vertexes neighbours

 ## Returns

 | Maximum Flow |
 | ------------ |
 | {s}`flow: T` |

 ```{note}
 The algorithm returns a single line with the value of the maximum flow between the source and target.
 ```

[^ref]: [A new approach to the Maximum-Flow Problem](http://akira.ruc.dk/~keld/teaching/algoritmedesign_f03/Artikler/08/Goldberg88.pdf)