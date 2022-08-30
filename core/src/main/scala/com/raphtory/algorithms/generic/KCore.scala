package com.raphtory.algorithms.generic

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.{Row, Table}

/*
* Step: check if already dead (i.e. degree < k), if so broadcast death to neighbours. Initialise effective degree as degree
* Iterate: if receive message saying neighbour died then decrement effective degree
*   Then check if the new degree is too low, in which case die and broadcast death
*
* */

class KCore(k: Int = 2) extends Generic {

  final val EFFDEGREE = "currKCoreDegree" // effective degree. TODO rename so number specific e.g. 3-core?
  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .step { vertex =>
        val degree = vertex.degree
        if (degree < k) { // the node dies, can't be in the k-core
          vertex.messageAllNeighbours(0) // here messaging 0 means the node has died
        }
        vertex.setState(EFFDEGREE, degree)
      }
      .iterate(
        { vertex =>
          val effDegree = vertex.getStateOrElse[Int](EFFDEGREE, vertex.degree) // todo is vertex.degree right as default?

          if (effDegree >= k) { // i.e. the node isn't already dead, in which case no updates needed
            val newlyDeadNeighbours = vertex.messageQueue[Int].length
            val newEffDegree = effDegree - newlyDeadNeighbours

            vertex.setState(EFFDEGREE, newEffDegree)
            if (newEffDegree < k) { // this vertex dies
              vertex.messageAllNeighbours(0)
            }
          }

        },
        iterations = 20,
        executeMessagedOnly = true
      )

  override def tabularise(graph: GraphPerspective): Table =
    graph
      .select(vertex =>
        Row(vertex.name, vertex.getState[Int](EFFDEGREE) >= k) //todo errorhandle or else here?
      ).filter(r => r.getBool(1))
}

object KCore {
  def apply(k: Int = 2) = new KCore(k)
}
