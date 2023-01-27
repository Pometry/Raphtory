package com.raphtory.examples.lotr.analysis

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Table
import com.raphtory.internals.communication.SchemaProviderInstances._

class DegreesSeparation(name: String = "Gandalf") extends Generic {

  final val SEPARATION = "SEPARATION"

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .step { vertex =>
        if (vertex.getPropertyOrElse("name", "") == name) {
          vertex.messageAllNeighbours(0)
          vertex.setState(SEPARATION, 0)
          vertex.setState("name", name)
        }
        else {
          vertex.setState(SEPARATION, -1)
          vertex.setState("name", vertex.name())
        }
      }
      .iterate(
              { vertex =>
                val sep_state   = vertex.messageQueue[Int].max + 1
                val current_sep = vertex.getStateOrElse[Int](SEPARATION, -1)
                if (current_sep == -1 & sep_state > current_sep) {
                  vertex.setState(SEPARATION, sep_state)
                  vertex.messageAllNeighbours(sep_state)
                }
              },
              iterations = 6,
              executeMessagedOnly = true
      )

  override def tabularise(graph: GraphPerspective): Table =
    graph
      .select("name", SEPARATION)
  //.filter(row=> row.getInt(1) > -1)
}

object DegreesSeparation {
  def apply(name: String = "Gandalf") = new DegreesSeparation(name)
}
