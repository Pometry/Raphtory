package com.raphtory.algorithms.generic

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.{Row, Table}
import com.raphtory.algorithms.generic.KCore.EFFDEGREE

/**
* {s}`Coreness(1, 6)`
* : Identify the maximal k for each vertex (the coreness, or k-core number) for which the vertex is in a k-core, here trying values of k from 1 to 6.
*
* https://en.wikipedia.org/wiki/Degeneracy_(graph_theory)
*
* ## Parameters
*
*  {s}`start: Int`
*    : The value of k to start trying values from (inclusive).
*
*  {s}`end: Int`
*    : The value of k to end at (inclusive).
*
* ## States
*
* {s}`CORENESS: Int`
* : Stores the k value of maximum k-core the vertex is in out of the k values that have been tried.
*
* ## Returns
*
* | vertex name       | coreness           |
* | ----------------- | ------------------ |
* | {s}`name: String` | {s}`CORENESS: Int` |
*
* ## Implementation
*
* The algorithm repeats KCore for start to end values of k, using the resetStates = false flag on KCore so it only runs on the nodes in the previous (as the KCores with k = 3 is a subgraph of the KCores with k = 2 etc).
* It updates the CORENESS state for all the nodes still alive in that KCore run, so the CORENESS state contains the highest value of k run of which that vertex was in the k-cores.
*/

class Coreness(start: Int, end: Int) extends Generic {
  final val CORENESS = "CORENESS"

  if (start > end | start < 0) { // out of range
    throw new IllegalArgumentException(s"start and end parameters must be non-negative and start <= end. You inputted start=$start, end=$end.")
  }
  override def apply(graph: GraphPerspective): graph.Graph = {
    var g = graph.step { vertex =>
      vertex.setState(CORENESS, 0)
    }
    for(k <- start to end) {
      g = KCore(k, resetStates = false).apply(g).step { vertex =>
        if (vertex.getState[Int](EFFDEGREE) >= k) { // if still in the core graph for this value of k, increment the coreness
          vertex.setState(CORENESS, k)
        }
      }
    }
    g
  }
  override def tabularise(graph: GraphPerspective): Table =
    graph
      .select(vertex =>
        {
        Row(vertex.name, vertex.getStateOrElse[Int](CORENESS, -1)
        )}
      )
}

object Coreness {
  def apply(start: Int = 1, end: Int = 3) = new Coreness(start, end)
}