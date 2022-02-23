package com.raphtory.algorithms.generic.centrality

import com.raphtory.algorithms.generic.NodeList
import com.raphtory.core.algorithm.GraphAlgorithm
import com.raphtory.core.algorithm.GraphPerspective
import com.raphtory.core.algorithm.Row
import com.raphtory.core.algorithm.Table

/**
  * {s}`WeightedPageRank(dampingFactor:Double = 0.85, iterateSteps:Int = 100, weightProperty = "weight")`
  *  : Compute PageRank for all nodes, treating the network as weighted
  *
  * Page Rank algorithm ranks nodes depending on their connections to determine how important
  * the node is. This assumes a node is more important if it receives more connections from others.
  * Each vertex begins with an initial state. If it has any neighbours, it sends them a message
  * which is the inital label / the number of neighbours.
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
  *  {s}`weightProperty: String = "weight"`
  *    : the property (if any) containing a numerical weight value for each edge, defaults to "weight".
  *
  *  ```{note}
  *  If the weight property is not found, weight is treated as the number of edge occurances.
  *  ```
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
  * [](com.raphtory.algorithms.generic.centrality.PageRank)
  * ```
  */
class WeightedPageRank(
    dampingFactor: Float = 0.85f,
    iterateSteps: Int = 100,
    weightProperty: String = "weight"
) extends NodeList(Seq("prlabel")) {

  override def apply(graph: GraphPerspective): GraphPerspective =
    graph
      .step { vertex =>
        val initLabel = 1.0f
        vertex.setState("prlabel", initLabel)
        val outWeight =
          vertex.getOutEdges().map(e => e.getPropertyOrElse(weightProperty, e.history().size)).sum
        vertex.getOutEdges().foreach { e =>
          vertex.messageVertex(
                  e.ID,
                  e.getPropertyOrElse(weightProperty, e.history().size).toFloat / outWeight
          )
        }
      }
      .iterate(
              { vertex =>
                val vname =
                  vertex.getPropertyOrElse("name", vertex.ID().toString) // for logging purposes
                val currentLabel = vertex.getState[Float]("prlabel")

                val queue    = vertex.messageQueue[Float]
                val newLabel = (1 - dampingFactor) + dampingFactor * queue.sum
                vertex.setState("prlabel", newLabel)

                val outWeight = vertex
                  .getOutEdges()
                  .map(e => e.getPropertyOrElse(weightProperty, e.history().size))
                  .sum
                vertex.getOutEdges().foreach { e =>
                  vertex.messageVertex(
                          e.ID,
                          newLabel * e
                            .getPropertyOrElse(weightProperty, e.history().size)
                            .toFloat / outWeight
                  )
                }

                if (Math.abs(newLabel - currentLabel) / currentLabel < 0.00001)
                  vertex.voteToHalt()
              },
              iterateSteps,
              false
      )
}

object WeightedPageRank {

  def apply(
      dampingFactor: Float = 0.85f,
      iterateSteps: Int = 100,
      weightProperty: String = "weight"
  ) =
    new WeightedPageRank(dampingFactor, iterateSteps)
}
