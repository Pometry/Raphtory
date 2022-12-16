package com.raphtory.algorithms.generic.motif

import com.raphtory.algorithms.generic.GraphState
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
  *  {s}`triangleCount: Int`
  *    : the number of triangles in the graph (treated as undirected and simple).
  *
  * ## Returns
  *
  *  | total triangles          |
  *  | ------------------------ |
  *  | {s}`triangleCount: Int`  |
  *
  * ```{note}
  *  Edges here are treated as undirected, so if the underlying network is directed here,
  * 'neighbours' refers to the union of in-neighbours and out-neighbours.
  * ``
  */

object GlobalTriangleCount extends GraphState(Seq("triangleCount")) {

  override def apply(graph: GraphPerspective): graph.Graph =
    LocalTriangleCount(graph)
      .setGlobalState(state => state.newAdder[Int]("triangles", retainState = true))
      .step { (vertex, state) =>
        val tri = vertex.getState[Int]("triangleCount")
        state("triangles") += tri
      }.setGlobalState(
      state => state.newConstant("triangleCount", state[Int,Int]("triangles").value/3)
    )
}
