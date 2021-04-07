package com.raphtory.algorithms

import com.raphtory.core.analysis.api.Analyser

import scala.collection.mutable.ArrayBuffer
import scala.util.Random


class WattsCascade(args:Array[String]) extends Analyser[Any](args){

  // Set initial seed of infected nodes
  val infectedSeed = Array(290,1459,65,1)

  // Random threshold or same threshold for all nodes
  val UNIFORM_RANDOM = 1
  val UNIFORM_SAMEVAL = 2
  val threshold = 0.1
  val r = new Random()

  override def setup(): Unit = {
    val threshold_choice = 1
    view.getVertices().foreach {
      vertex =>
        if (infectedSeed.contains(vertex.ID())) {
          vertex.setState("infected",true)
          vertex.messageAllNeighbours(1.0)
        }
        else {
          vertex.setState("infected",false)
        }
        if (threshold_choice == UNIFORM_RANDOM) {
          vertex.setState("threshold", r.nextFloat())
        } else {
          vertex.setState("threshold",threshold)
        }
    }
  }

  override def analyse(): Unit = {
    view.getVertices().foreach { vertex =>
      val degree = vertex.getIncEdges.size + vertex.getOutEdges.size
      val newLabel = if (degree > 0 && !vertex.getState[Boolean]("infected")) (vertex.messageQueue[Double].sum/degree > threshold) else vertex.getState[Boolean]("infected")
      if (newLabel != vertex.getState[Boolean]("infected")) {
        vertex.messageAllNeighbours(1.0)
        vertex.setState("infected",true)
      } else vertex.voteToHalt()
    }
  }

  override def returnResults(): Any = {
    val infected = view.getVertices().map {
      vertex => if (vertex.getState[Boolean]("infected")) 1 else 0
    }
    (infected.size, infected.sum)
  }

  override def defineMaxSteps(): Int = 100

  override def extractResults(results: List[Any]): Map[String,Any]  = {
    val endResults = results.asInstanceOf[ArrayBuffer[(Int, Int)]]
    val totalV = endResults.map (x => x._1).sum
    val totalInfected = endResults.map (x => x._2).sum
    val propInfected = if (totalV > 0) totalInfected.toDouble/totalV.toDouble else 0

    val text = s"""{"totalV":$totalV,"cascadeSize":$totalInfected,"cascadeProp":$propInfected}"""
    println(text)
    publishData(text)
    Map[String,Any]()
  }

}
