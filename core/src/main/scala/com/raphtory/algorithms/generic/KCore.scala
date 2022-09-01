package com.raphtory.algorithms.generic

import com.raphtory.algorithms.generic.KCore.EFFDEGREE
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.{Row, Table}

/**
  * {s}`KCore(3)`
  * : Identify the K-Cores of a graph with given k
  *
  * A k-core of a graph is a maximal subgraph in which every vertex has degree at least k. This algorithm identifies the k-cores of a graph.
  * (https://en.wikipedia.org/w/index.php?title=Degeneracy_(graph_theory)&action=edit&section=3).
  *
  * ## Parameters
  *
  *  {s}`k: Int`
  *    : The value of k to run k-core with.
  *
  *  {s}`resetStates: Bool = True`
  *    : When true this resets all the effectiveDegree states at the start of running so the algorithm runs from scratch, when false reuses previous effectiveDegree data and runs on the remaining alive nodes (e.g. for nested KCore iterations like in Coreness.)
  *    : Defaults to true, generally should be true.
  *
  * ## States
  *
  * {s}`effectiveDegree: Int`
  * : Stores the current number of alive neighbours of each vertex (decreases as nodes get deleted).
  * : At the end of the algorithm the nodes in the k-cores are those with effectiveDegree => k.
  * : In a tabularise this can be filtered using the boolean {s}`vertex.getState[Int]("effectiveDegree") >= k`
  *
  * ## Returns
  *
  * | vertex name       | if in the k-core (only returns true rows)               |
  * | ----------------- | ------------------------------------------------------- |
  * | {s}`name: String` | {s}`vertex.getState[Int]("effectiveDegree") >= k: Bool` |
  *
  * ## Implementation
  *
  * The algorithm works by recursively removing nodes that don't have enough alive neighbours for them to remain in the graph.
  *
  *  1. Each node sets is effectiveDegree to it's actual degree (as all it's neighbours are currently alive). If it has degree < k, it can't be in the k-core, so dies and broadcasts its death.
  *     If resetStates == false, we start from previous effectiveDegree values instead.
  *
  *  2. When a node receives a message saying its neighbour has died, it decrements its effective degree.
  *
  *  3. It then checks if its effectiveDegree < k, in which case it can't be in the k-core, so dies and broadcasts its death.
  *
  *  4. This continues until no more nodes are dying.
  *
  */
class KCore(k: Int, resetStates: Boolean = true) extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .step { vertex =>

        if ((resetStates == false) & (vertex.containsState(EFFDEGREE))) {  // i.e. you're reusing the previous states
          // we assume here that the previous iteration of KCore was run with a smaller value of k (this allows e.g. Coreness to run)
          // therefore the effective degree is only going to decrease for each node, so any node with a previous effective degree = k-1
          // (so this node was alive last time i.e. is counted in other nodes' effective degree, but can't be in the current k core: dies straight )
          val effDegree = vertex.getStateOrElse(EFFDEGREE, vertex.degree)
          if (effDegree == k-1) { // i.e. in previous round was alive but dead straight away in this round
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
        iterations = 100,
        executeMessagedOnly = true
      )

  override def tabularise(graph: GraphPerspective): Table =
    graph
      .select(vertex =>
        Row(vertex.name, vertex.getState[Int](EFFDEGREE) >= k)
      ).filter(r => r.getBool(1))
}

object KCore {

  final val EFFDEGREE = "effectiveDegree"
  def apply(k: Int, resetStates: Boolean = true) = new KCore(k, resetStates)

}
