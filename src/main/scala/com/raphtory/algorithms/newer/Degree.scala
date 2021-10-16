package com.raphtory.algorithms.newer

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

class Degree(path:String) extends GraphAlgorithm{
  override def algorithm(graph: GraphPerspective): Unit = {
    graph.select(vertex =>
    Row(vertex.getPropertyOrElse("name", vertex.ID()), vertex.getInNeighbours().size, vertex.getOutNeighbours().size, vertex.getAllNeighbours().size))
      .writeTo(path)
  }
}

object Degree{
  def apply(path:String) = new Degree(path)
}