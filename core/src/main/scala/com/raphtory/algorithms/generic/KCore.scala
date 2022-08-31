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

class KCore(k: Int, resetStates: Boolean = true) extends Generic {

  final val EFFDEGREE = "effectiveDegree" // effective degree

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .step { vertex =>

        if ((resetStates == false) & (vertex.containsState(EFFDEGREE))) {  // i.e. you're reusing the previous states
          // we assume here that the previous iteration of KCore was run with a smaller value of k (this allows e.g. Coreness to run)
          // therefore the effective degree is only going to decrease for each node, so any node with a previous effective degree < current k dies straight away
          val effDegree = vertex.getStateOrElse(EFFDEGREE, vertex.degree)
          if (effDegree < k) {
            vertex.messageAllNeighbours(0)
          }
        } else {
          val degree = vertex.degree
          if (degree < k) { // the node dies, can't be in the k-core
            vertex.messageAllNeighbours(0) // here messaging 0 means the node has died
          }
          vertex.setState(EFFDEGREE, degree)
        }

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
  def apply(k: Int, resetStates: Boolean = true) = new KCore(k, resetStates)
}
