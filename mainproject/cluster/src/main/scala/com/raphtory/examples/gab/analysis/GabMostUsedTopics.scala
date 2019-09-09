package com.raphtory.examples.gab.analysis

import akka.actor.ActorContext
import com.raphtory.core.analysis.API.{Analyser, WorkerID}
import com.raphtory.core.utils.Utils.resultNumeric
import monix.execution.atomic.AtomicDouble

import scala.collection.mutable.ArrayBuffer

class GabMostUsedTopics extends Analyser {
  private var epsilon        = 1
  private val dumplingFactor = 0.85F
  private var firstStep      = true

  override def setup() = {}

  override def analyse() : ArrayBuffer[(String, Int, String)] = {
    //println("Analyzing")
    var results = ArrayBuffer[(String, Int, String)]()
    proxy.getVerticesSet().foreach(v => {
      val vertex = proxy.getVertex(v)
      if(vertex.getPropertyCurrentValue("type").getOrElse("no type").equals("topic")){
        val ingoingNeighbors  = vertex.getIngoingNeighbors.size
        results.synchronized {
          vertex.getPropertyCurrentValue("id") match {
            case None =>
            case Some(id) =>
              results +:= (id.toString, ingoingNeighbors, vertex.getPropertyCurrentValue("title").getOrElse("no title").toString)
          }
          if (results.size > 10) {
            results = results.sortBy(_._2)(Ordering[Int].reverse).take(10)
          }
        }
    }})
    //println("Sending step end")
    results
  }

  override def defineMaxSteps(): Int = 1

  override def processResults(results: ArrayBuffer[Any], oldResults: ArrayBuffer[Any]): Unit =  {
    println()
    println("Current top topics")
    results.asInstanceOf[ArrayBuffer[ArrayBuffer[(String, Int, String)]]].flatten.sortBy(f => f._2)(Ordering[Int].reverse).foreach(
      topic => println(s"Topic: ${topic._3} with ID ${topic._1} and total uses of ${topic._2}")
    )
    println()
  }
  override def processViewResults(results: ArrayBuffer[Any], oldResults: ArrayBuffer[Any], timestamp: Long): Unit = {}
  override def processWindowResults(results: ArrayBuffer[Any], oldResults: ArrayBuffer[Any], timestamp: Long, windowSize: Long): Unit = {}


  override def checkProcessEnd(results:ArrayBuffer[Any],oldResults:ArrayBuffer[Any]) : Boolean = {
    try {
      val _newResults = results.asInstanceOf[ArrayBuffer[ArrayBuffer[(Long, Double)]]].flatten
      val _oldResults = oldResults.asInstanceOf[ArrayBuffer[ArrayBuffer[(Long, Double)]]].flatten
      println(s"newResults: ${_newResults.size} => ${_oldResults.size}")

      if (firstStep) {
        firstStep = false
        return false
      }

      val newSum = _newResults.sum(resultNumeric)._2
      val oldSum = _oldResults.sum(resultNumeric)._2

      println(s"newSum = $newSum - oldSum = $oldSum - diff = ${newSum - oldSum}")
      //results = _newResults
      Math.abs(newSum - oldSum) / _newResults.size < epsilon
    } catch {
      case _ : Exception => false
    }
  }

}
