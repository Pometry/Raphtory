package com.raphtory.examples.twitter.analysis

import com.raphtory.algorithms.api.GraphAlgorithm
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table

/**
  * Description
  * Page Rank algorithm ranks nodes depending on their connections to determine how important
  * the node is. This assumes a node is more important if it receives more connections from others.
  * Each vertex begins with an initial state. If it has any neighbours, it sends them a message
  * which is the initial label / the number of neighbours.
  * Each vertex, checks its messages and computes a new label based on: the total value of
  * messages received and the damping factor. This new value is propagated to all outgoing neighbours.
  * A vertex will stop propagating messages if its value becomes stagnant (i.e. has a change of less
  * than 0.00001) This process is repeated for a number of iterate step times. Most algorithms should
  * converge after approx. 20 iterations.
  *
  * Parameters
  * dampingFactor (Double) : Probability that a node will be randomly selected by a user traversing the graph, defaults to 0.85.
  * iterateSteps (Int) : Number of times for the algorithm to run.
  * output (String) : The path where the output will be saved. If not specified, defaults to /tmp/PageRank
  *
  * Returns
  * ID (Long) : Vertex ID
  * Page Rank (Double) : Rank of the node
  */
class PageRank(
    dampingFactor: Double = 0.85,
    iterateSteps: Int = 100,
    convergenceRate: Double = 0.00001,
    output: String = "/tmp/PageRank"
) extends GraphAlgorithm {

  override def apply(graph: GraphPerspective): GraphPerspective =
    graph
      .step { vertex =>
        val initLabel = 1.0
        vertex.setState("prlabel", initLabel)
        val outDegree = vertex.getOutNeighbours().size
        if (outDegree > 0.0)
          vertex.messageOutNeighbours(initLabel / outDegree)
      }
      .iterate(
              { vertex =>
                val vname        = vertex.name() // for logging purposes
                val currentLabel = vertex.getState[Double]("prlabel")

                val queue    = vertex.messageQueue[Double]
                val newLabel = (1 - dampingFactor) + dampingFactor * queue.sum
                vertex.setState("prlabel", newLabel)

                val outDegree = vertex.getOutNeighbours().size
                if (outDegree > 0)
                  vertex.messageOutNeighbours(newLabel / outDegree)

                if (Math.abs(newLabel - currentLabel) < convergenceRate)
                  vertex.voteToHalt()
              },
              iterateSteps,
              false
      ) // make iterate act on all vertices, not just messaged ones

  override def tabularise(graph: GraphPerspective): Table =
    graph.select { vertex =>
      Row(
              vertex.name(),
              vertex.getStateOrElse("prlabel", -1)
      )
    }
}

object PageRank {

  def apply(
      dampingFactor: Double = 0.85,
      iterateSteps: Int = 100,
      convergenceRate: Double = 0.00001,
      output: String = "/tmp/PageRank"
  ) =
    new PageRank(dampingFactor, iterateSteps, convergenceRate, output)
}
