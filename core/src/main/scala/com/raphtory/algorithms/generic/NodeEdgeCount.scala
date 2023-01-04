package com.raphtory.algorithms.generic

import com.raphtory.api.analysis.algorithm.{GenericReduction, GraphStateOutput}
import com.raphtory.api.analysis.graphstate.Accumulator
import com.raphtory.api.analysis.graphview.GraphPerspective

import scala.math.Ordering.Implicits._

/**
  * {s}`NodeEdgeCount`
  *  Stores/returns the number of nodes and edges in the graph.
  *
  *  This counts the number of nodes and edges in the perspective and returns them. We count both the number of "undirected"
  *  edges, treating the graph as a simple undirected graph, the number of directed edges, treating the graph as directed
  *  and simple, and the number of temporal edges which includes duplicate directed edges between the same pair of nodes.
  *
  * ## States
  *
  *  {s}`numNodes: Int`
  *  : Number of nodes in the perspective
  *
  *  {s}`directedEdges: Int`
  *  : Number of directed edges in the perspective
  *
  *  {s}`undirectedEdges: Int`
  *  : Number of undirected edges in the perspective
  *
  *  {s}`temporalEdges: Int`
  *  : Number of directed edges with multiplicity in the perspective
  *
  *
  *  ## Returns
  *
  *  | no nodes          | no directed edges       | no undirected edges       | no temporal edges |
  *  | ----------------- | ----------------------- | ------------------------- | ----------------- |
  *  | {s}`numNodes: Int` | {s}`directedEdges: Int` | {s}`undirectedEdges: Int` | {s}`temporalEdges: Int` |
  *
  */

object NodeEdgeCount extends GraphStateOutput(Seq("numNodes", "directedEdges", "undirectedEdges", "temporalEdges")) with GenericReduction {

  override def apply(graph: GraphPerspective): graph.ReducedGraph = {
    graph.reducedView.setGlobalState({state =>
      state.newIntAdder("directedEdges", 0,retainState = true)
      state.newIntAdder("undirectedEdgesAcc", 0, retainState = true)
      state.newIntAdder("temporalEdges", 0, retainState = true)
      state.newConstant("numNodes", state.nodeCount)
    }).step{ (vertex, state) =>
      import vertex._
      val acc1: Accumulator[Int, Int] = state("directedEdges")
      acc1+=vertex.outEdges.size
      val acc2: Accumulator[Int, Int] = state("undirectedEdgesAcc")
      acc2+=vertex.degree
      val acc3: Accumulator[Int,Int] = state("temporalEdges")
      acc3+=vertex.explodeOutEdges().size
    }
      .setGlobalState(state => state.newConstant("undirectedEdges",state[Int,Int]("undirectedEdgesAcc").value/2))
  }
}