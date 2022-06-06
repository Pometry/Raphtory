package com.raphtory.algorithms.generic.dynamic

import com.raphtory.algorithms.generic.NodeList
import com.raphtory.api.analysis.graphview.GraphPerspective

import scala.util.Random

/**
  * {s}`DiscreteSI(infectedNode: Iterable[String], infectionProbability: Double = 0.5, maxGenerations: Int = 100, seed:Long = -1)`
  *    : discrete susceptible-infected (SI) model on the network
  *
  *  The network is treated as directed. An infected node propagates the infection along each
  *  of its out-edges with probability `infectionProbability`. The propagation terminates once there are no newly infected
  *  nodes or `maxGenerations` is reached. The initially infected seed nodes are considered generation 0.
  *
  * ## Parameters
  *
  *  {s}`infectedNode: Seq[String]`
  *    : names of initially infected nodes
  *
  *  {s}`infectionProbability: Double = 0.5`
  *    : probability of infection propagation along each edge
  *
  *  {s}`seed: Long`
  *    : seed for random number generator (specify for deterministic results)
  *
  *  {s}`maxGenerations: Int = 100`
  *    : maximum number of propagation generations
  *
  * ## States
  *
  *  {s}`infected: Boolean`
  *    : infection status of vertex
  *
  *  {s}`generation: Int`
  *    : generation at which vertex became infected (unset for vertices that were never infected)
  *
  * ## Returns
  *
  * | vertex name       | infection status       |
  * | ----------------- | ---------------------- |
  * | {s}`name: String` | {s}`infected: Boolean` |
  */
class DiscreteSI(
    infectedNodes: Set[String],
    infectionProbability: Double = 0.5,
    maxGenerations: Int = 50,
    seed: Long = -1
) extends NodeList(Seq("infected")) {

  override def apply(graph: GraphPerspective): graph.Graph = {
    val randomiser = if (seed != -1) new Random(seed) else new Random()

    graph
      .step { vertex =>
        if (infectedNodes contains vertex.name()) {
          vertex.setState("infected", true)
          vertex.setState("generation", 0)
          vertex
            .getOutEdges()
            .foreach(edge => if (randomiser.nextFloat() < infectionProbability) edge.send(1))
        }
        else
          vertex.setState("infected", false)
      }
      .iterate(
              { vertex =>
                val infected = vertex.getState[Boolean]("infected")
                if (infected)
                  vertex.voteToHalt()
                else {
//            all messages are the same, simply get first to get generation count
                  val generation = vertex.messageQueue[Int].head
                  vertex.setState("infected", true)
                  vertex.setState("generation", generation)
                  vertex.getOutEdges().foreach { edge =>
                    if (randomiser.nextFloat() < infectionProbability) edge.send(generation + 1)
                  }
                }
              },
              maxGenerations,
              true
      )
  }
}

object DiscreteSI {

  def apply(
      infectedNode: Iterable[String],
      infectionProbability: Double = 0.5,
      maxGenerations: Int = 100,
      seed: Long = -1
  ) =
    new DiscreteSI(
            infectedNodes = infectedNode.toSet,
            infectionProbability = infectionProbability,
            maxGenerations = maxGenerations,
            seed = seed
    )
}
