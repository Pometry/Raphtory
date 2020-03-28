package com.raphtory.examples.gab.analysis

import com.raphtory.core.analysis.API.Analyser

import scala.collection.mutable.ArrayBuffer

class GabMostUsedTopics extends Analyser {
  private var epsilon        = 1
  private val dumplingFactor = 0.85f
  private var firstStep      = true

  override def setup(): Unit = {}

  override def analyse(): Unit = {
    //println("Analyzing")
    var results = ArrayBuffer[(String, Int, String)]()
    proxy.getVerticesSet().foreach { v =>
      val vertex = proxy.getVertex(v._2)
      if (vertex.getPropertyCurrentValue("type").getOrElse("no type").equals("topic")) {
        val ingoingNeighbors = vertex.getIngoingNeighbors.size
        results.synchronized {
          vertex.getPropertyCurrentValue("id") match {
            case None =>
            case Some(id) =>
              results +:= (id.toString, ingoingNeighbors, vertex
                .getPropertyCurrentValue("title")
                .getOrElse("no title")
                .toString)
          }
          if (results.size > 10)
            results = results.sortBy(_._2)(Ordering[Int].reverse).take(10)
        }
      }
    }
    //println("Sending step end")
    results
  }

  override def defineMaxSteps(): Int = 1

  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
    println()
    println("Current top topics")
    results
      .asInstanceOf[ArrayBuffer[ArrayBuffer[(String, Int, String)]]]
      .flatten
      .sortBy(f => f._2)(Ordering[Int].reverse)
      .foreach(topic => println(s"Topic: ${topic._3} with ID ${topic._1} and total uses of ${topic._2}"))
    println()
  }
  override def processViewResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {}
  override def processWindowResults(
      results: ArrayBuffer[Any],
      timestamp: Long,
      windowSize: Long,
      viewCompleteTime: Long
  ): Unit = {}

  override def returnResults(): Any = ???
}
