package com.raphtory.algorithms

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

class NeighbourAverageDegree(path:String) extends GraphAlgorithm{
  override def algorithm(graph: GraphPerspective): Unit = {
    graph.step({
      vertex =>
        val degree = vertex.getAllNeighbours().size
        vertex.messageAllNeighbours(degree)
    })
      .select({
        vertex =>
          val degrees = vertex.messageQueue[Int]
          val degree = vertex.getAllNeighbours().size
          val avgNeigh = if (degree > 0) degrees.sum.toFloat/degree else 0.0
          Row(vertex.getPropertyOrElse("name", vertex.ID()), degree, avgNeigh)
      })
      .writeTo(path)
  }
}

object NeighbourAverageDegree{
  def apply(path:String) = new NeighbourAverageDegree(path:String)
}