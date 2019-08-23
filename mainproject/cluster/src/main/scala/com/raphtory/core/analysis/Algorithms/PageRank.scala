package com.raphtory.core.analysis.Algorithms

import com.raphtory.core.analysis.{Analyser, WorkerID}
import com.raphtory.core.model.communication.VertexMessage
import com.raphtory.core.utils.Utils.resultNumeric

import scala.collection.mutable.ArrayBuffer

class PageRank extends Analyser {

  private val prStr              = "_pageRank"
  private val outgoingCounterStr = "_outgoingCounter"
  private val dumplingFactor = 0.85F
  private var firstStep      = true

  private val defaultPR          = (1F)          // Initial PR for t = 0
  private val defaultOC          = "1"                                // Default outgoing links counter to be used as divisor
  //private val constantPRop       = (1 - dumplingFactor) / networkSize // Constant to be added for each iteration in the PR
  case class PageRankScore(value:Float) extends VertexMessage

  private def getPageRankStr(srcId : Int, dstId : Int) : String = s"${prStr}_${srcId}_$dstId"

  override def setup()(implicit workerID: WorkerID) = {
    proxy.getVerticesSet().foreach(v => {
      val vertex = proxy.getVertex(v)
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

  override def defineMaxSteps(): Int = 10

  override def processResults(results: ArrayBuffer[Any], oldResults: ArrayBuffer[Any]): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[(Long,ArrayBuffer[(Long, Float)])]]
    val top5 = endResults.map(x => x._2).flatten.sortBy(f => f._2)(Ordering[Float].reverse).take(5)
    val topTime = new java.util.Date(endResults.map(x => x._1).max)
    println (s"At $topTime the Users with the highest rank were $top5")

  }

  override def processViewResults(results: ArrayBuffer[Any], oldResults: ArrayBuffer[Any], timestamp: Long): Unit = {}

  override def processWindowResults(results: ArrayBuffer[Any], oldResults: ArrayBuffer[Any], timestamp: Long, windowSize: Long): Unit = {}

  override def checkProcessEnd(results:ArrayBuffer[Any],oldResults:ArrayBuffer[Any]) : Boolean = {
    try {
      val _newResults = results.asInstanceOf[ArrayBuffer[ArrayBuffer[(Long, Double)]]].flatten
      val _oldResults = oldResults.asInstanceOf[ArrayBuffer[ArrayBuffer[(Long, Double)]]].flatten
      val epsilon : Float = 0.85F
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
