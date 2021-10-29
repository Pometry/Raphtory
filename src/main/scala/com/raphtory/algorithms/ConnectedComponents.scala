package com.raphtory.algorithms

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

class ConnectedComponents(path:String) extends GraphAlgorithm{
  override def algorithm(graph: GraphPerspective): Unit = {
    val size = graph.nodeCount()
    graph
      .step({
        vertex =>
          println(size)
          vertex.setState("cclabel", vertex.ID)
          vertex.messageAllNeighbours(vertex.ID)
      })
      .iterate({
        vertex =>
          val label = vertex.messageQueue[Long].min
          if (label < vertex.getState[Long]("cclabel")) {
            vertex.setState("cclabel", label)
            vertex messageAllNeighbours label
          }
          else
            vertex.voteToHalt()
      }, 100)
      .select(vertex => Row(vertex.ID(),vertex.getState[Long]("cclabel")))
      .writeTo(path)
  }
}

object ConnectedComponents{
  def apply(path:String) = new ConnectedComponents(path)
}
