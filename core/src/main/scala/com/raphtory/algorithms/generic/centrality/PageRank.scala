package com.raphtory.algorithms.generic.centrality

import com.raphtory.algorithms.generic.NodeList
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.internals.communication.SchemaProviderInstances._

/**
  * {s}`PageRank(dampingFactor:Double = 0.85, iterateSteps:Int = 100)`
  *  : Compute PageRank for all nodes
  *
  * Page Rank algorithm ranks nodes depending on their connections to determine how important
  * the node is. This assumes a node is more important if it receives more connections from others.
  * Each vertex begins with an initial state. If it has any neighbours, it sends them a message
  * which is the initial label / the number of neighbours.
  * Each vertex, checks its messages and computes a new label based on: the total value of
  * messages received and the damping factor. This new value is propogated to all outgoing neighbours.
  * A vertex will stop propogating messages if its value becomes stagnant (i.e. has a change of less
  * than 0.00001) This process is repeated for a number of iterate step times. Most algorithms should
  * converge after approx. 20 iterations.
  *
  * ## Parameters
  *
  *  {s}`dampingFactor: Double = 0.85`
  *    : Probability that a node will be randomly selected by a user traversing the graph, defaults to 0.85.
  *
  *  {s}`iterateSteps: Int = 100`
  *    : Maximum number of iterations for the algorithm to run.
  *
  * ## States
  *
  *  {s}`prlabel: Double`
  *    : PageRank of the node
  *
  * ## Returns
  *
  *  | vertex name       | PageRank             |
  *  | ----------------- | -------------------- |
  *  | {s}`name: String` | {s}`prlabel: Double` |
  *
  * ```{seealso}
  * [](com.raphtory.algorithms.generic.centrality.WeightedPageRank
  * ```
  */
class PageRank(dampingFactor: Double = 0.85, iterateSteps: Int = 100, tol: Double = 0.00001)
        extends NodeList(Seq("prlabel")) {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .setGlobalState { state =>
        state.newAdder[Double]("dangling")
      }
      .step { (vertex, state) =>
        val initLabel = 1.0
        vertex.setState("prlabel", initLabel)
        val outDegree = vertex.outDegree
        if (outDegree > 0.0)
          vertex.messageOutNeighbours(initLabel / outDegree)
        else state("dangling") += initLabel / state.nodeCount
      }
      .iterate(
              { (vertex, state) =>
                val currentLabel     = vertex.getState[Double]("prlabel")
                val dangling: Double = state("dangling").value
                val queue            = vertex.messageQueue[Double]
                val newLabel         = (1 - dampingFactor) + dampingFactor * (queue.sum + dangling)
                vertex.setState("prlabel", newLabel)
                val outDegree        = vertex.outDegree

                if (outDegree > 0)
                  vertex.messageOutNeighbours(newLabel / outDegree)
                else
                  state("dangling") += newLabel / state.nodeCount

                if (Math.abs(newLabel - currentLabel) < tol)
                  vertex.voteToHalt()
              },
              iterateSteps,
              false
      ) // make iterate act on all vertices, not just messaged ones
}

object PageRank {

  def apply(dampingFactor: Double = 0.85, iterateSteps: Int = 100, tol: Double = 0.00001) =
    new PageRank(dampingFactor, iterateSteps, tol)
}
