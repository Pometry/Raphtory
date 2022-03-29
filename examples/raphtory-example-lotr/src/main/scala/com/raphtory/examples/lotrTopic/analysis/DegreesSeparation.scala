package com.raphtory.examples.lotrTopic.analysis

import com.raphtory.core.algorithm.{GraphAlgorithm, GraphPerspective, Row, Table}

class DegreesSeparation(name: String = "Gandalf") extends GraphAlgorithm {

  final val SEPARATION = "SEPARATION"

  override def apply(graph: GraphPerspective): GraphPerspective = {
    graph
      .step({
        vertex =>
          if (vertex.getPropertyOrElse("name", "") == name) {
            vertex.messageAllNeighbours(0)
            vertex.setState(SEPARATION, 0)
          } else {
            vertex.setState(SEPARATION, -1)
          }
      })
      .iterate(
        {
          vertex =>
            val sep_state = vertex.messageQueue[Int].max + 1
            val current_sep = vertex.getStateOrElse[Int](SEPARATION, -1)
            if (current_sep == -1 & sep_state > current_sep) {
              vertex.setState(SEPARATION, sep_state)
              vertex.messageAllNeighbours(sep_state)
            }
        }, iterations = 6, executeMessagedOnly = true)
  }

  override def tabularise(graph: GraphPerspective): Table = {
    graph
      .select(vertex => Row(vertex.getPropertyOrElse("name", "unknown"), vertex.getStateOrElse[Int](SEPARATION, -1)))
    //.filter(row=> row.getInt(1) > -1)
  }
}


object DegreesSeparation{
  def apply(name: String = "Gandalf") = new DegreesSeparation(name)
}

