package com.raphtory.algorithms.generic

import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.visitor.Vertex

import scala.util.Random

class BinaryDiffusion(
    infectedNodes: Set[String] = Set[String](),
    seed: Long = -1
) extends NodeList(Seq("infected")) {

  private val infectedStatus     = "infected"
  private val randomiser: Random = if (seed != -1) new Random(seed) else new Random()

  def randomlyInfect(vertex: Vertex): Unit =
    vertex.outEdges.foreach(edge => if (randomiser.nextBoolean()) edge.send(infectedStatus))

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .step { vertex =>
        // if no infected nodes are given then randomly infect all nodes
        if (infectedNodes.size == 0) {
          if (randomiser.nextBoolean()) {
            vertex.setState(infectedStatus, true)
            randomlyInfect(vertex)
          }
        }
        else if (infectedNodes.contains(vertex.name())) {
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
                  else
                    vertex.voteToHalt()
              },
              iterations = 100,
              executeMessagedOnly = true
      )
}

object BinaryDiffusion {

  def apply(infectedNodes: Set[String] = Set[String](), seed: Long = -1) =
    new BinaryDiffusion(infectedNodes, seed)
}
