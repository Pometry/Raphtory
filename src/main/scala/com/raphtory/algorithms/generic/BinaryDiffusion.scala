package com.raphtory.algorithms.generic

import com.raphtory.core.algorithm.GraphPerspective
import com.raphtory.core.graph.visitor.Vertex

import scala.util.Random

class BinaryDiffusion(
    infectedNodes: Set[Long] = Set[Long](),
    seed: Long = -1,
    reinfect: Boolean = true
) extends NodeList(Seq("infected")) {

  val infectedStatus     = "infected"
  val previouslyInfected = "prevInfected"
  val randomiser: Random = if (seed != -1) new Random(seed) else new Random()

  def randomlyInfect(vertex: Vertex): Unit =
    vertex.getOutEdges().foreach(edge => if (randomiser.nextBoolean()) edge.send(infectedStatus))

  override def apply(graph: GraphPerspective): GraphPerspective =
    graph
      .step { vertex =>
        // if no infected nodes are given then randomly infect all nodes
        if (infectedNodes.size == 0) {
          if (randomiser.nextBoolean()) {
            vertex.setState(infectedStatus, true)
            randomlyInfect(vertex)
          }
        } else if (infectedNodes.contains(vertex.ID())) {
          vertex.setState(infectedStatus, true)
          randomlyInfect(vertex)
        }
      }
      .iterate(
              { vertex =>
                val messages = vertex.messageQueue.distinct
                if (messages.contains(infectedStatus))
                  // never been infected, then set infected and randomly infect others
                  if (!vertex.getStateOrElse[Boolean](infectedStatus, false)) {
                    vertex.setState(infectedStatus, true)
                    randomlyInfect(vertex)
                  }
                  // if you are already infected and want to repeat reinfection
                  else if (vertex.getState[Boolean](infectedStatus) & reinfect)
                    randomlyInfect(vertex)
                  // if you are already infected and dont want to repeat infection
                  else if (vertex.getState[Boolean](infectedStatus) & !reinfect)
                    vertex.voteToHalt()

              },
              iterations = 100,
              executeMessagedOnly = true
      )
}

object BinaryDiffusion {

  def apply(infectedNodes: Set[Long] = Set[Long](), seed: Long = -1, reinfect: Boolean = true) =
    new BinaryDiffusion(infectedNodes, seed, reinfect)
}
