package com.raphtory.examples.gab.analysis

import akka.actor.ActorContext
import com.raphtory.core.analysis.{Analyser, GraphRepoProxy}
import monix.execution.atomic.AtomicDouble

import scala.concurrent.Await
import scala.util.Random

class GabPageRank3(networkSize : Int, dumplingFactor : Float) extends Analyser {

  private val prStr              = "_pageRank"
  private val outgoingCounterStr = "_outgoingCounter"

  private val defaultPR          = (1F/networkSize).toString          // Initial PR for t = 0
  private val defaultOC          = "1"                                // Default outgoing links counter to be used as divisor
  private val constantPRop       = (1 - dumplingFactor) / networkSize // Constant to be added for each iteration in the PR


  private def getPageRankStr(srcId : Int, dstId : Int) : String = s"${prStr}_${srcId}_$dstId"

  override def setup() = {
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

  override def analyse() : Vector[(Long, Double)] = {
    println("Analyzing")
    var results = Vector.empty[(Long, Double)]
    proxy.getVerticesSet().foreach(v => {
      val vertex = proxy.getVertex(v)
      val pageRank : AtomicDouble = AtomicDouble(constantPRop)
      val ingoingNeighbors  = vertex.getIngoingNeighbors
      val outgoingNeighbors = vertex.getOutgoingNeighbors
      ingoingNeighbors.foreach(u => {
        val previousPrU        = vertex.getIngoingNeighborProp(u, getPageRankStr(u, v.toInt)).getOrElse(defaultPR).toDouble
        val outgoingCounterU   = vertex.getIngoingNeighborProp(u, outgoingCounterStr).getOrElse(defaultOC).toDouble
        pageRank.getAndAdd(dumplingFactor * previousPrU / outgoingCounterU) // PR(v, t + 1)
        Thread.sleep(0,10)
      })

      if (pageRank.get == Double.PositiveInfinity || pageRank.get == Double.NegativeInfinity)
        pageRank.set(defaultPR.toDouble)
      outgoingNeighbors.foreach(u => {
        vertex.pushToOutgoingNeighbor(u, getPageRankStr(v.toInt, u), pageRank.get.toString)
        Thread.sleep(0, 10)
      })
      results.synchronized {
        results
        results +:= (v.toLong, pageRank.get)
        if (results.size > 10) {
          results = results.sortBy(_._2)(Ordering[Double].reverse).take(10)
        }
      }
    })
    println("Sending step end")
    results
  }
}
