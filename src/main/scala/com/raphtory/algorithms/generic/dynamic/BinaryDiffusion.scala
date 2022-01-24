package com.raphtory.algorithms.generic.dynamic

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row, Table}

import scala.util.Random

class BinaryDiffusion(infectedNode:Long, seed:Long, outputFolder:String) extends GraphAlgorithm {
  override def apply(graph: GraphPerspective): GraphPerspective = {
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
      }, 100, true)
  }

  override def tabularise(graph: GraphPerspective): Table = {
    graph.select(vertex => Row(vertex.name(), vertex.getStateOrElse("infected", false)))
      .filter(row => row.get(1) == true)
  }

  override def write(table: Table): Unit = {
    table.writeTo(outputFolder)
  }
}

object BinaryDiffusion {
  def apply(infectedNode:Long, seed:Long = -1, outputFolder:String =  "/tmp/binaryDiffusion") =
    new BinaryDiffusion(infectedNode, seed, outputFolder)
}
