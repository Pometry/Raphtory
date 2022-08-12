package com.raphtory.algorithms.generic

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.{Row, Table}

import scala.math.Ordering.Implicits._

/**
  * {s}`NodeEdgeCount`
  *  Stores/returns the number of nodes and edges in the graph.
  *
  *  This counts the number of nodes and edges in the perspective and returns them. We count both the number of "undirected"
  *  edges, treating the graph as a simple undirected graph, and the number of directed edges, treating the graph as directed
  *  and simple.
  *
  * ## States
  *
  *  {s}`directedEdges: Long`
  *  : Number of directed edges in the perspective
  *
  *  {s}`undirectedEdges: Long`
  *  : Number of undirected edges in the perspective
  *
  *  | no nodes          | no directed edges       | no undirected edges       |
  *  | ----------------- | ----------------------- | ------------------------- |
  *  | {s}`noNodes: Int` | {s}`directedEdges: Int` | {s}`undirectedEdges: Int` |
  *
  */

object NodeEdgeCount extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph = {
    graph.setGlobalState({state =>
      state.newAdder[Int]("directedEdges")
      state.newAdder[Int]("undirectedEdges")
    }).step{ (vertex, state) =>
      import vertex._
      state("directedEdges")+=vertex.outEdges.size
      state("undirectedEdges")+=vertex.neighbours.count(_ > vertex.ID)
    }
  }

  override def tabularise(graph: GraphPerspective): Table =
    graph.globalSelect{
      state =>
        Row(state.nodeCount, state("directedEdges").value, state("undirectedEdges").value)
    }
}

