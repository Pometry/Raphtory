package com.raphtory.core.analysis.Algorithms

import com.raphtory.core.analysis.API.Analyser

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable
import scala.util.Random

class BinaryDefusion(args:Array[String]) extends Analyser(args) {
  val infectedNode = 31
  override def setup(): Unit =
    view.getVerticesSet().foreach { vertex =>
      if (vertex.ID() == infectedNode) {
        val toSend = vertex.getOrSetCompValue("infected", view.superStep()).asInstanceOf[Int]
        vertex.getOutgoingNeighbors.foreach { neighbour =>
          if (Random.nextBoolean())
            vertex.messageNeighbour(neighbour._1, toSend)
        }
      }
    }

  override def analyse(): Unit =
    view.getVerticesWithMessages().foreach { vertex =>
      vertex.clearQueue
      if (vertex.containsCompValue("infected"))
        vertex.voteToHalt() //already infected
      else {
        val toSend = vertex.getOrSetCompValue("infected", view.superStep()).asInstanceOf[Int]
        vertex.getOutgoingNeighbors.foreach { neighbour =>
          if (Random.nextBoolean())
            vertex.messageNeighbour(neighbour._1, toSend)
        }
      }
    }

  override def returnResults(): Any =
    view.getVerticesSet().map { vertex =>
        if (vertex.containsCompValue("infected"))
          (vertex.ID, vertex.getCompValue("infected").asInstanceOf[Int])
        else
          (-1, -1)

      }
      .filter(f => f._2 >= 0)

  override def defineMaxSteps(): Int = 100

  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = ???

  override def processViewResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Long, Int]]].flatten
    println(endResults)
    println(endResults.size)
  }
}
