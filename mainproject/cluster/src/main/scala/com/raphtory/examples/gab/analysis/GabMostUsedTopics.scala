package com.raphtory.examples.gab.analysis

import akka.actor.ActorContext
import com.raphtory.core.analysis.Analyser
import com.raphtory.core.storage.GraphRepoProxy
import monix.execution.atomic.AtomicDouble

class GabMostUsedTopics(networkSize : Int, dumplingFactor : Float) extends Analyser {

  override def setup()(implicit proxy : GraphRepoProxy.type) = {}

  override def analyse()(implicit proxy : GraphRepoProxy.type, managerCount : Int) : Vector[(String, Int, String)] = {
    //println("Analyzing")
    var results = Vector.empty[(String, Int, String)]
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

  override implicit var context: ActorContext = _
  override implicit var managerCount: Int = _
}
