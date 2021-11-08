package com.raphtory.algorithms

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

class TriangleCount(path:String) extends GraphAlgorithm {

  override def algorithm(graph: GraphPerspective): Unit = {
    graph.step({
      vertex =>
        vertex.setState("triangles",0)
        val neighbours = vertex.getAllNeighbours().toSet
        neighbours.foreach({
          nb =>
            vertex.messageNeighbour(nb, neighbours)
        })
    })
      .select({
        vertex =>
          val neighbours = vertex.getAllNeighbours().toSet
          val queue = vertex.messageQueue[Set[Long]]
          var tri = 0
          queue.foreach(
            nbs =>
              tri+=nbs.intersect(neighbours).size
          )
          vertex.setState("triangles",tri/2)
          Row(vertex.getPropertyOrElse("name", vertex.ID()), vertex.getState[Int]("triangles"))
      })
      .writeTo(path)
  }
}

object TriangleCount{
  def apply(path:String) = new TriangleCount(path:String)
}
