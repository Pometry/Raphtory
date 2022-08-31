package com.raphtory.algorithms.generic

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.{Row, Table}
import com.raphtory.algorithms.generic.KCore

class Coreness(start: Int, end: Int) extends Generic {
  // start and end determine the range of k values that KCore runs to determine the coreness
  final val CORENESS = "CORENESS"
  final val EFFDEGREE = "effectiveDegree" // todo get dynamically from KCORE?

  // todo add checks
  override def apply(graph: GraphPerspective): graph.Graph = {
    var g = graph.step { vertex =>
      println("hello world")
      vertex.setState(CORENESS, 0)
    }
    for(k <- start to end) {
      g = KCore(k, resetStates = false).apply(g).step { vertex =>
        if (vertex.getStateOrElse[Int](EFFDEGREE, 0) >= k) { // if still in the core graph for this value of k, increment the coreness //todo error handle better
          vertex.setState(CORENESS, k)
        }
      }
    }
    g
    //graph.asInstanceOf[graph.Graph] // TODO type mismatch with just graph: found "graph.type", required: "graph.Graph"
  }

  override def tabularise(graph: GraphPerspective): Table =
    graph
      .select(vertex =>{println("tab")
        Row(vertex.name, vertex.getStateOrElse[Int](CORENESS, -1))}
      )
}

object Coreness {
  def apply(start: Int = 1, end: Int = 3) = new Coreness(start, end)
}