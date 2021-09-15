package com.raphtory.examples.gab.analysis

import com.raphtory.core.analysis.api.Analyser

import scala.collection.mutable.ArrayBuffer

class GabMostUsedTopics(args:Array[String]) extends Analyser[Any](args){
  private var epsilon        = 1
  private val dumplingFactor = 0.85f
  private var firstStep      = true

  override def setup(): Unit = {}

  override def analyse(): Unit = {
    //println("Analyzing")
    var results = ArrayBuffer[(String, Int, String)]()
    view.getVertices().foreach { vertex =>
      if (vertex.getPropertyValue("type").getOrElse("no type").equals("topic")) {
        val ingoingNeighbors = vertex.getInEdges().size
        results.synchronized {
          vertex.getPropertyValue("id") match {
            case None =>
            case Some(id) =>
              results +:= (id.toString, ingoingNeighbors, vertex
                .getPropertyValue("title")
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

  override def extractResults(results: List[Any]): Map[String,Any]  = {
    println()
    println("Current top topics")
    results
      .asInstanceOf[ArrayBuffer[ArrayBuffer[(String, Int, String)]]]
      .flatten
      .sortBy(f => f._2)(Ordering[Int].reverse)
      .foreach(topic => println(s"Topic: ${topic._3} with ID ${topic._1} and total uses of ${topic._2}"))
    println()
    Map[String,Any]()
  }

  override def returnResults(): Any = ???
}
