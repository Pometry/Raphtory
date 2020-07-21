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
    view.getVertices().foreach { vertex =>
        val walletID = vertex.getPropertyValue("id").get.asInstanceOf[String]
      if(walletID equals infectedNode) {
          vertex.getOrSetState("infected", infectionStartingBlock)
          vertex.getOrSetState("infectedBy", "Start")
          vertex.getOutEdgesAfter(infectionStartingBlock).foreach { neighbour =>
            neighbour.send((walletID,neighbour.getFirstActivityAfter(infectionStartingBlock)))
          }
        }
    }

  override def analyse(): Unit =
    view.getMessagedVertices().foreach { vertex =>
      var infectionBlock = infectionStartingBlock
      var infector = infectedNode
      val queue  = vertex.messageQueue[(String,Long)]
      infectionBlock = queue.map(x=>x._2).min
      infector = queue.filter(x=>x._2==infectionBlock).head._1 //todo check if multiple

      //if (vertex.containsCompValue("infected"))
      //  vertex.voteToHalt() //already infected
      //else {
      val walletID = vertex.getPropertyValue("id").get.asInstanceOf[String]
      vertex.getOrSetState("infected", infectionBlock)
      vertex.getOrSetState("infectedBy",infector)
      vertex.getOutEdgesAfter(infectionBlock).foreach { neighbour =>
        neighbour.send((walletID,neighbour.getFirstActivityAfter(infectionBlock)))
      }
      //}
    }

  override def returnResults(): Any =
    view
      .getVertices().map { vertex =>
        if (vertex.containsState("infected"))
          (vertex.getPropertyValue("id").get.asInstanceOf[String], vertex.getState("infected").asInstanceOf[Long],vertex.getState("infectedBy").asInstanceOf[String])
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
    var data = s"{block:$timeStamp,edges:["
    for (elem <- endResults)
      data+=s"""{"infected":"${elem._1}","block":"${elem._2}","infector":"${elem._3}"}"""
    data+="]}"
    publishData(data)
    //for (elem <- endResults) {    Utils.writeLines(s"/Users/mirate/Documents/phd/etheroutput/block${timeStamp}.csv", s"${elem._1},${elem._2},${elem._3},", "")}

  }


}
