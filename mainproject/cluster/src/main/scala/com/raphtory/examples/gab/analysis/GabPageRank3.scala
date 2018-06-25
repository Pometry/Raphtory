package com.raphtory.examples.gab.analysis

import akka.actor.ActorContext
import com.raphtory.core.analysis.Analyser
import com.raphtory.core.storage.controller.GraphRepoProxy

import scala.util.Random

class GabPageRank3(networkSize : Int, dumplingFactor : Float) extends Analyser {

  private val prStr              = "_pageRank"
  private val outgoingCounterStr = "_outgoingCounter"

  private val defaultPR          = (1F/networkSize).toString          // Initial PR for t = 0
  private val defaultOC          = "1"                                // Default outgoing links counter to be used as divisor
  private val constantPRop       = (1 - dumplingFactor) / networkSize // Constant to be added for each iteration in the PR


  private def getPageRankStr(srcId : Int, dstId : Int) : String = s"${prStr}_${srcId}_$dstId"

  override def setup()(implicit proxy : GraphRepoProxy.type) = {
    proxy.getVerticesSet().foreach(v => {
      val vertex = proxy.getVertex(v)
      vertex.updateProperty(prStr, defaultPR)
      val outgoings = vertex.getOutgoingNeighbors
      outgoings.foreach(u => {
        vertex.pushToOutgoingNeighbor(u, getPageRankStr(v.toInt, u), defaultPR)       // t = 0
        vertex.pushToOutgoingNeighbor(u, outgoingCounterStr, outgoings.size.toString) // t = 0
      })
    })
  }

  override def analyse()(implicit proxy : GraphRepoProxy.type, managerCount : Int) : Vector[(Long, Double)] = {
    println("Analyzing")
    var results = Vector.empty[(Long, Double)]
    proxy.getVerticesSet().foreach(v => {
      val vertex = proxy.getVertex(v)
      var pageRank : Double = constantPRop
      val ingoingNeighbors  = vertex.getIngoingNeighbors
      val outgoingNeighbors = vertex.getOutgoingNeighbors
      ingoingNeighbors.foreach(u => {
        val previousPrU        = vertex.getIngoingNeighborProp(u, getPageRankStr(u, v.toInt)).getOrElse(defaultPR).toDouble
        val outgoingCounterU   = vertex.getIngoingNeighborProp(u, outgoingCounterStr).getOrElse(defaultOC).toDouble
        pageRank += dumplingFactor * previousPrU / outgoingCounterU // PR(v, t + 1)
      })
      outgoingNeighbors.foreach(u =>
        vertex.pushToOutgoingNeighbor(u, getPageRankStr(v.toInt, u), pageRank.toString)
      )
      results +:= (v, pageRank)
      if (results.size > 10) {
        results = results.sortBy(_._2)(Ordering[Double].reverse).take(10)
      }
    })
    println("Sending step end")
    results
  }

  override implicit var context: ActorContext = _
  override implicit var managerCount: Int = _
}
