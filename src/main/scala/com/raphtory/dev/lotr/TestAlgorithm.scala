package com.raphtory.dev.lotr

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

class TestAlgorithm extends GraphAlgorithm{
  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .step({
        vertex =>
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
      .select(vertex => Row(Array(vertex.ID(),vertex.getState[Long]("cclabel"))))
      .filter(r=> r.get(0).asInstanceOf[Long]==18174)
      .writeTo("")
  }
}
object TestAlgorithm{
  def apply() = new TestAlgorithm()
}
