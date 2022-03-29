package com.raphtory.algorithms.generic.centrality

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row, Table}

class NeighbourAverageDegree(path:String) extends GraphAlgorithm{
  override def apply(graph: GraphPerspective): GraphPerspective = {
    graph.step({
      vertex =>
        val degree = vertex.getAllNeighbours().size
        vertex.messageAllNeighbours(degree)
    })
  }

  override def tabularise(graph: GraphPerspective): Table = {
    graph.select({
      vertex =>
        val degrees = vertex.messageQueue[Int]
        val degree = vertex.getAllNeighbours().size
        val avgNeigh = if (degree > 0) degrees.sum.toFloat / degree else 0.0
        Row(vertex.name(), degree, avgNeigh)
    })
  }

  override def write(table: Table): Unit = {
    table.writeTo(path)
  }
}

object NeighbourAverageDegree{
  def apply(path:String) = new NeighbourAverageDegree(path:String)
}