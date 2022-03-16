package com.raphtory.algorithms.generic.centrality

import com.raphtory.algorithms.generic.NodeList
import com.raphtory.core.algorithm.GraphAlgorithm
import com.raphtory.core.algorithm.GraphPerspective
import com.raphtory.core.algorithm.Row
import com.raphtory.core.algorithm.Table

/**
  * {s}`AverageNeighbourDegree()`
  *  : Compute the average degree of a vertex's neighbours
  *
  *  ```{note}
  *  This algorithm treats the network as undirected.
  *  ```
  *
  * ## States
  *
  *  {s}`avgNeighbourDegree: Double`
  *    : Average degree of the vertex's neighbours
  *
  * ## Returns
  *
  *  | vertex name       | average neighbour degree     |
  *  | ----------------- | ---------------------------- |
  *  | {s}`name: String` | `avgNeighbourDegree: Double` |
  */
class AverageNeighbourDegree extends NodeList(Seq("avgNeighbourDegree")) {

  override def apply(graph: GraphPerspective): GraphPerspective =
    graph.step(vertex => {
      if (vertex.ID() == 543457135497636503L) {
        val x = 1
      }
      vertex.messageAllNeighbours(vertex.degree)
    }).step { vertex =>
      val degrees = vertex.messageQueue[Int]
      vertex.setState(
              "avgNeighbourDegree",
              if (vertex.degree > 0) degrees.sum.toFloat / vertex.degree else 0.0
      )
    }
}

object AverageNeighbourDegree {
  def apply() = new AverageNeighbourDegree
}
