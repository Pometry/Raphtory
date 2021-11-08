package com.raphtory.algorithms

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

class Degree(path:String) extends GraphAlgorithm{
  override def algorithm(graph: GraphPerspective): Unit = {
    graph.select({
      vertex =>
      val inDegree = vertex.getInNeighbours().size
      val outDegree = vertex.getOutNeighbours().size
      val totalDegree = vertex.getAllNeighbours().size
    Row(vertex.getPropertyOrElse("name", vertex.ID()), inDegree, outDegree, totalDegree)
    })
      .writeTo(path)
  }
}

object Degree{
  def apply(path:String) = new Degree(path)
}