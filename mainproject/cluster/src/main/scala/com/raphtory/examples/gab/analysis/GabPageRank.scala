package com.raphtory.examples.gab.analysis

import akka.actor.{ActorContext, ActorRef}
import com.raphtory.core.analysis.Analyser
import com.raphtory.core.storage.GraphRepoProxy

import scala.collection.concurrent.TrieMap
import scala.util.Random

class GabPageRank(networkSize : Int, epsilon : Float, delta1 : Float) extends Analyser {
  private val c : Float = 2 / (delta1 * epsilon)
  private val K = c * Math.log(networkSize)

  private val couponCountStr = "_couponCount"
  private val visitsCountStr = "_visitsCounter"
  private val randomWalksStr = "_randomWalks"
  private val rnd = new Random()

  private def getRandomWalkString(v : Long, u : Long) = s"${randomWalksStr}_${v}_${u}"

  override def setup()(implicit proxy : GraphRepoProxy.type) = {
    proxy.getVerticesSet().foreach(v => {
      proxy.getVertex(v).updateProperty(visitsCountStr, K.toString) // Row 2 : Set Cv = K
    })
  }

  override def analyse()(implicit proxy : GraphRepoProxy.type, managerCount : Int) : Vector[(Long, Double)] = {
    println("Analyzing")
    var results = Vector.empty[(Long, Double)]
    proxy.getVerticesSet().foreach(v => {
      val vertex = proxy.getVertex(v)
      val couponCount : Double = vertex.getPropertyCurrentValue(couponCountStr).getOrElse(K.toString).toDouble
      var pageRank : Double = couponCount * epsilon / (c*networkSize * Math.log(networkSize))
      if (couponCount > 0) {
        val outgoingNeighbors = vertex.getOutgoingNeighbors
        val Tvu = TrieMap.empty[Int, Int]
        for (i <- 0 until couponCount.round.toInt
           if rnd.nextFloat() < 1 - epsilon) {
          var max = outgoingNeighbors.size
          if (max < 1)
            max = 0
          val u = outgoingNeighbors.iterator.drop(rnd.nextInt(outgoingNeighbors.size)).next()
          Tvu.get(u) match {
            case Some(value) => Tvu.put(u, value + 1)
            case None        => Tvu.put(u, 1)
          }
        }
        outgoingNeighbors.foreach(u =>
          vertex.pushToOutgoingNeighbor(u, getRandomWalkString(v, u), Tvu.getOrElse(u, 0).toString))
        val totalNumberOfVisitsInRound = Tvu.map(e => e._2).sum
        vertex.updateProperty(visitsCountStr, (couponCount + totalNumberOfVisitsInRound).toString)
        vertex.updateProperty(couponCountStr, totalNumberOfVisitsInRound.toString)
        pageRank = (couponCount + totalNumberOfVisitsInRound) * epsilon / (c*networkSize * Math.log(networkSize))
      }
      results :+= (v, pageRank)
    })
    println("Sending step end")
    println(results)
    results.sortBy(f => f._2)(Ordering[Double]).take(10)
  }

  override implicit var context: ActorContext = _
  override implicit var managerCount: Int = _
}
