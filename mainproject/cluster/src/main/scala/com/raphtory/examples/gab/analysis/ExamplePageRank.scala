package com.raphtory.examples.gab.analysis

import akka.actor.ActorContext
import com.raphtory.core.analysis.{Analyser, GraphRepoProxy, Worker}
import com.raphtory.core.model.communication.VertexMessage
import monix.execution.atomic.AtomicDouble

import scala.concurrent.Await
import scala.util.Random

class ExamplePageRank(networkSize : Int, dumplingFactor : Float) extends Analyser {

  private val prStr              = "_pageRank"
  private val outgoingCounterStr = "_outgoingCounter"

  private val defaultPR          = (1F)          // Initial PR for t = 0
  private val defaultOC          = "1"                                // Default outgoing links counter to be used as divisor
  //private val constantPRop       = (1 - dumplingFactor) / networkSize // Constant to be added for each iteration in the PR
  case class PageRankScore(value:Float) extends VertexMessage

  private def getPageRankStr(srcId : Int, dstId : Int) : String = s"${prStr}_${srcId}_$dstId"

  override def setup()(implicit workerID: Worker) = {
    proxy.getVerticesSet().foreach(v => {
      val vertex = proxy.getVertex(v)
      vertex.updateProperty(prStr, defaultPR.toString)
      val outgoings = vertex.getOutgoingNeighbors
      vertex.messageAllOutgoingNeighbors(PageRankScore(defaultPR))
    })
  }

  override def analyse()(implicit workerID: Worker) : Vector[(Long, Float)] = {
    var results = Vector.empty[(Long, Float)]
    println(proxy.getVerticesSet().size)
    proxy.getVerticesSet().foreach(v => {
      val vertex = proxy.getVertex(v)
      var neighbourScores = 0F
      while(vertex moreMessages)
        neighbourScores += vertex.nextMessage().asInstanceOf[PageRankScore].value
      val newPR:Float = neighbourScores/vertex.getIngoingNeighbors.size
      vertex.updateProperty(prStr, newPR.toString)
      vertex messageAllOutgoingNeighbors(PageRankScore(newPR))
      results +:= (v.toLong, newPR)
      })

    if (results.size > 10) {
      results = results.sortBy(_._2)(Ordering[Float].reverse).take(10)
    }

    results
  }
}
