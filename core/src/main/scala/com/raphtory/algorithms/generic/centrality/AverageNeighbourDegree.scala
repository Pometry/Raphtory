package com.raphtory.algorithms.generic.centrality

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Table
import com.raphtory.internals.communication.SchemaProviderInstances._

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
object AverageNeighbourDegree extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .step(vertex => vertex.messageAllNeighbours(vertex.degree))
      .step { vertex =>
        val degrees = vertex.messageQueue[Int]
        vertex.setState(
                "avgNeighbourDegree",
                if (vertex.degree > 0) degrees.sum.toFloat / vertex.degree else 0.0
        )
      }

  override def tabularise(graph: GraphPerspective): Table =
    graph
      .select("name", "avgNeighbourDegree")
}
