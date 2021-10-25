package com.raphtory.algorithms.newer

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}
import scala.util.Random

class BinaryDiffusion(infectedNode:Long, seed:Long, outputFolder:String) extends GraphAlgorithm {
  override def algorithm(graph: GraphPerspective): Unit = {
    val randomiser = if (seed != -1) new Random(seed) else new Random()
    val infectedStatus = "infected"
    graph
      .step({
        vertex =>
          if (vertex.ID() == infectedNode) {
            vertex.setState(infectedStatus, true)
            vertex.getOutEdges().foreach {
              edge => if (randomiser.nextBoolean()) edge.send(infectedStatus)
            }
          }
      })
      .iterate({
        vertex =>
          val messages = vertex.messageQueue.distinct
          if (messages contains infectedStatus) {
            vertex.setState(infectedStatus, true)
            vertex.getOutEdges().foreach {
              edge => if (randomiser.nextBoolean()) edge.send(infectedStatus)
            }
          }
      }, 100)
      .select(vertex => Row(vertex.ID(), vertex.getStateOrElse("infected", false)))
      .filter(row => row.get(1) == true)
      .writeTo(outputFolder)
  }
}

object BinaryDiffusion {
  def apply(infectedNode:Long, seed:Long = -1, outputFolder:String =  "/tmp/binaryDiffusion") =
    new BinaryDiffusion(infectedNode, seed, outputFolder)
}
