package com.raphtory.examples.blockchain.analysers

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.utils.Utils

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable
import scala.util.Random

class EthereumTaintTracking(args:Array[String]) extends Analyser(args) {
  //val infectedNode =System.getenv().getOrDefault("ETHEREUM_TAINT_TRACKING_BAD_ACTOR", "0xa09871aeadf4994ca12f5c0b6056bbd1d343c029").trim
  //val infectionStartingBlock =System.getenv().getOrDefault("ETHEREUM_TAINT_TRACKING_START_BLOCK", "9007863").trim.toLong
  //val infectedNode =System.getenv().getOrDefault("ETHEREUM_TAINT_TRACKING_BAD_ACTOR", "0x52bc44d5378309EE2abF1539BF71dE1b7d7bE3b5").trim.toLowerCase.asInstanceOf[String]
  //val infectionStartingBlock =System.getenv().getOrDefault("ETHEREUM_TAINT_TRACKING_START_BLOCK", "4000000").trim.toLong
  val infectedNode = args(0).trim.toLowerCase
  val infectionStartingBlock = args(1).trim.toLong
  override def setup(): Unit =
    proxy.getVerticesSet().foreach { v =>
        val vertex = proxy.getVertex(v._2)
        val walletID = vertex.getPropertyCurrentValue("id").get.asInstanceOf[String]
      if(walletID equals infectedNode) {
          vertex.getOrSetCompValue("infected", infectionStartingBlock)
          vertex.getOrSetCompValue("infectedBy", "Start")
          vertex.getOutgoingNeighborsAfter(infectionStartingBlock).foreach { neighbour =>
            vertex.messageNeighbour(neighbour._1, (walletID,neighbour._2.getTimeAfter(infectionStartingBlock)))
          }
        }
    }

  override def analyse(): Unit =
    proxy.getVerticesWithMessages().foreach { vert =>
      val vertex = proxy.getVertex(vert._2)
      var infectionBlock = infectionStartingBlock
      var infector = infectedNode
      val queue  = vertex.messageQueue.map(_.asInstanceOf[(String,Long)])
      if (queue.nonEmpty) {
        infectionBlock = queue.map(x=>x._2).min
        infector = queue.filter(x=>x._2==infectionBlock).head._1 //todo check if multiple
        vertex.clearQueue
      }
      //if (vertex.containsCompValue("infected"))
      //  vertex.voteToHalt() //already infected
      //else {
      val walletID = vertex.getPropertyCurrentValue("id").get.asInstanceOf[String]
      vertex.getOrSetCompValue("infected", infectionBlock)
      vertex.getOrSetCompValue("infectedBy",infector)
      vertex.getOutgoingNeighborsAfter(infectionBlock).foreach { neighbour =>
        vertex.messageNeighbour(neighbour._1, (walletID,neighbour._2.getTimeAfter(infectionBlock)))
      }
      //}
    }

  override def returnResults(): Any =
    proxy
      .getVerticesSet()
      .map { vert =>
        val vertex = proxy.getVertex(vert._2)
        if (vertex.containsCompValue("infected"))
          (vertex.getPropertyCurrentValue("id").get.asInstanceOf[String], vertex.getCompValue("infected").asInstanceOf[Long],vertex.getCompValue("infectedBy").asInstanceOf[String])
        else
          ("", -1L,"")

      }
      .filter(f => f._2 >= 0).par

  override def defineMaxSteps(): Int = 100

  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParIterable[(String, Long,String)]]].flatten
    //println(s"Run as of ${System.currentTimeMillis()}")
    for (elem <- endResults) {println(s"${elem._1},${elem._2},${elem._3},")}
    }
  override def processViewResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParIterable[(String, Long,String)]]].flatten
    //println(s"Completed block $timeStamp")
    for (elem <- endResults) {    Utils.writeLines(s"/Users/mirate/Documents/phd/etheroutput/block${timeStamp}.csv", s"${elem._1},${elem._2},${elem._3},", "")}

  }


}
