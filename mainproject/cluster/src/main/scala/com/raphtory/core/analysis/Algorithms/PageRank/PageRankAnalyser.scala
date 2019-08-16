package com.raphtory.core.analysis.Algorithms.PageRank

import com.raphtory.core.analysis.{Analyser, WorkerID}
import com.raphtory.core.model.communication.VertexMessage

import scala.collection.mutable.ArrayBuffer

class PageRankAnalyser(networkSize : Int, dumplingFactor : Float) extends Analyser {

  private val prStr              = "_pageRank"
  private val outgoingCounterStr = "_outgoingCounter"

  private val defaultPR          = (1F)          // Initial PR for t = 0
  private val defaultOC          = "1"                                // Default outgoing links counter to be used as divisor
  //private val constantPRop       = (1 - dumplingFactor) / networkSize // Constant to be added for each iteration in the PR
  case class PageRankScore(value:Float) extends VertexMessage

  private def getPageRankStr(srcId : Int, dstId : Int) : String = s"${prStr}_${srcId}_$dstId"

  override def setup()(implicit workerID: WorkerID) = {
    proxy.getVerticesSet().foreach(v => {
      val vertex = proxy.getVertex(v)
//      if(vertex.messageQueue.nonEmpty)
//        println(vertex.messageQueue)
      val toSend = vertex.getOrSetCompValue(prStr, defaultPR).asInstanceOf[Float]
      vertex.messageAllOutgoingNeighbors(PageRankScore(toSend))
    })
  }

  override def analyse()(implicit workerID: WorkerID) : (Long,ArrayBuffer[(Long, Float)]) = {
    var results = ArrayBuffer[(Long, Float)]()
    proxy.getVerticesSet().foreach(v => {
      val vertex = proxy.getVertex(v)
      var neighbourScores = 0F
      while(vertex moreMessages)
        neighbourScores += vertex.nextMessage().asInstanceOf[PageRankScore].value

      val newPR:Float = neighbourScores/math.max(vertex.getOutgoingNeighbors.size,1)
      vertex.setCompValue(prStr, newPR)
      vertex messageAllOutgoingNeighbors(PageRankScore(newPR))
      results +:= (v.toLong, newPR)
      })
    if (results.size > 5) {
      results = results.sortBy(_._2)(Ordering[Float].reverse).take(5)
    }
    (proxy.latestTime,results)
  }
}
