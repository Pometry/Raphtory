//package com.raphtory.examples.random.depricated
//
//import com.raphtory.core.analysis.API.Analyser
//
//import scala.collection.mutable.ArrayBuffer
//
//class PageRank(args:Array[String]) extends Analyser(args){
//
//  private val prStr              = "_pageRank"
//  private val outgoingCounterStr = "_outgoingCounter"
//  private val dumplingFactor     = 0.85f
//  private var firstStep          = true
//
//  private val defaultPR = (1f) // Initial PR for t = 0
//  private val defaultOC = "1"  // Default outgoing links counter to be used as divisor
//  //private val constantPRop       = (1 - dumplingFactor) / networkSize // Constant to be added for each iteration in the PR
//
//  private def getPageRankStr(srcId: Int, dstId: Int): String = s"${prStr}_${srcId}_$dstId"
//
//  override def setup(): Unit =
//    proxy.getVerticesSet().foreach { v =>
//      val vertex = proxy.getVertex(v._2)
//      val toSend = vertex.getOrSetCompValue(prStr, defaultPR).asInstanceOf[Float]
//      vertex.messageAllOutgoingNeighbors(toSend)
//    }
//
//  override def analyse(): Unit = {
//    var results = ArrayBuffer[(Long, Float)]()
//    proxy.getVerticesSet().foreach { v =>
//      val vertex          = proxy.getVertex(v._2)
//      var neighbourScores = 0f
//      // while(vertex moreMessages)
//      //    neighbourScores += vertex.nextMessage().asInstanceOf[PageRankScore].value
//
//      val newPR: Float = neighbourScores / math.max(vertex.getOutgoingNeighbors.size, 1)
//      vertex.setCompValue(prStr, newPR)
//      vertex messageAllOutgoingNeighbors (newPR)
//      results +:= (v._1.toLong, newPR)
//    }
//    if (results.size > 5)
//      results = results.sortBy(_._2)(Ordering[Float].reverse).take(5)
//    (proxy.latestTime, results)
//  }
//
//  override def defineMaxSteps(): Int = 10
//
//  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
//    val endResults = results.asInstanceOf[ArrayBuffer[(Long, ArrayBuffer[(Long, Float)])]]
//    val top5       = endResults.map(x => x._2).flatten.sortBy(f => f._2)(Ordering[Float].reverse).take(5)
//    val topTime    = new java.util.Date(endResults.map(x => x._1).max)
//    println(s"At $topTime the Users with the highest rank were $top5")
//
//  }
//
//  override def processViewResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {}
//
//  override def processWindowResults(
//      results: ArrayBuffer[Any],
//      timestamp: Long,
//      windowSize: Long,
//      viewCompleteTime: Long
//  ): Unit = {}
//
//  override def returnResults(): Any = ???
//}
