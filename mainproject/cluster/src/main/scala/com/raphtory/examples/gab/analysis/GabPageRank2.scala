package com.raphtory.examples.gab.analysis

import akka.actor.ActorContext
import com.raphtory.core.analysis.{Analyser, GraphRepoProxy}

import scala.collection.concurrent.TrieMap
import scala.util.Random

class GabPageRank2(networkSize : Int, epsilon : Float, delta1 : Float) extends Analyser {
  private val c : Float = 1F

  private val prStr = "_pagerank"
  private val rnd = new Random()
  private val defaultPR = "1"
  private def getPageRankStr(srcId : Int, dstId : Int) : String = s"${prStr}_${srcId}_${dstId}"

  override def setup() = {
    proxy.getVerticesSet().foreach(v => {
      val vertex = proxy.getVertex(v)
      vertex.updateProperty(prStr, defaultPR)
      vertex.getOutgoingNeighbors.foreach(u =>
        vertex.pushToOutgoingNeighbor(u, getPageRankStr(v.toInt, u), defaultPR))
    })
  }

  override def analyse() : Vector[(Long, Double)] = {
    println("Analyzing")
    var results = Vector.empty[(Long, Double)]
    proxy.getVerticesSet().foreach(v => {
      val vertex = proxy.getVertex(v)
      var pageRank : Double = 0D
      val ingoingNeighbors = vertex.getIngoingNeighbors
      println(s"Inside vertex $v")
      ingoingNeighbors.foreach(u => {
        val y = vertex.getIngoingNeighborProp(u, getPageRankStr(u, v.toInt)).getOrElse(defaultPR).toDouble
        println(s"  Adding $y to pagerank from $u")
        pageRank += c * vertex.getIngoingNeighborProp(u, getPageRankStr(u, v.toInt)).getOrElse(defaultPR).toDouble
      })
      val outgoings = vertex.getOutgoingNeighbors
      var x = outgoings.size
      if (x < 1)
        x = 1
      println(s"  Dividing $pageRank by $x")
      pageRank /= x
      outgoings.foreach(u =>
        vertex.pushToOutgoingNeighbor(u, getPageRankStr(v.toInt, u), pageRank.toString)
      )
      results +:= (v.toLong, pageRank.toDouble)
    })
    println("Sending step end")
    println(results)
    results.sortBy(f => f._2)(Ordering[Double]).take(10)
  }
}
