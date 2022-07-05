package com.raphtory.algorithms.generic.motif

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

/**
  * {s}`GlobalTriangleCount()`
  *  : Returns the total number of undirected triangles in the graph.
  *
  * ## States
  *
  *  {s}`triangles: Int`
  *    : three times the number of triangles (but returned in output without the factor of three).
  *
  * ## Returns
  *
  *  | total triangles      |
  *  | -------------------- |
  *  | {s}`triangles: Int`  |
  *
  * ```{note}
  *  Edges here are treated as undirected, so if the underlying network is directed here,
  * 'neighbours' refers to the union of in-neighbours and out-neighbours.
  * ``
  */

object GlobalTriangleCount extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    LocalTriangleCount(graph)
      .setGlobalState(state => state.newAdder[Int]("triangles", retainState = true))
      .step { (vertex, state) =>
        val tri = vertex.getState[Int]("triangleCount")
        state("triangles") += tri
      }

  override def tabularise(graph: GraphPerspective): Table =
    graph.globalSelect { state =>
      val totalTri: Int = state("triangles").value
      Row(totalTri / 3)
    }
}
