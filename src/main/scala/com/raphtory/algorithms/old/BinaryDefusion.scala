package com.raphtory.algorithms.old

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable
import scala.util.Random

class BinaryDefusion(args:Array[String]) extends Analyser[Any](args) {
  val infectedNode = 31
  override def setup(): Unit =
    view.getVertices().foreach { vertex =>
      if (vertex.ID() == infectedNode) {
        val toSend = vertex.getOrSetState("infected", view.superStep).asInstanceOf[Int]
        vertex.getOutEdges().foreach { neighbour =>
          if (Random.nextBoolean())
            vertex.messageNeighbour(neighbour.ID(), toSend)
        }
      }
    }

  override def analyse(): Unit =
    view.getMessagedVertices().foreach { vertex =>
      //vertex.clearQueue //todo this should probably happen by default
      if (vertex.containsState("infected"))
        vertex.voteToHalt() //already infected
      else {
        val toSend = vertex.getOrSetState("infected", view.superStep).asInstanceOf[Int]
        vertex.getOutEdges().foreach { neighbour =>
          if (Random.nextBoolean())
            vertex.messageNeighbour(neighbour.ID(), toSend)
        }
      }
    }

  override def returnResults(): Any =
    view.getVertices().map { vertex =>
        if (vertex.containsState("infected"))
          (vertex.ID, vertex.getState("infected").asInstanceOf[Int])
        else
          (-1, -1)

      }
      .filter(f => f._2 >= 0)

  override def defineMaxSteps(): Int = 100

  override def extractResults(results: List[Any]): Map[String, Any] = {
    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Long, Int]]].flatten
    Map[String,Any]()
  }

}
