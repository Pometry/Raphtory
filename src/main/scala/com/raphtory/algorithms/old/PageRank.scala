package com.raphtory.algorithms.old

import scala.collection.mutable.ArrayBuffer

class PageRank(args:Array[String]) extends Analyser[(Int, List[(Long,Double)])](args) {
  object sortOrdering extends Ordering[Double] {
    def compare(key1: Double, key2: Double) = key2.compareTo(key1)
  }

  // damping factor, (1-d) is restart probability
  val d = 0.85

  override def setup(): Unit =
    view.getVertices().foreach { vertex =>
      val outEdges = vertex.getOutEdges()
      val outDegree = outEdges.size
      if (outDegree > 0) {
        val toSend = 1.0/outDegree
        vertex.setState("prlabel",toSend)
        outEdges.foreach(edge => {
          edge.send(toSend)
        })
      } else {
        vertex.setState("prlabel",0.0)
      }
    }

  override def analyse(): Unit =
    view.getMessagedVertices().foreach {vertex =>
      val currentLabel = vertex.getState[Double]("prlabel")
      val newLabel = 1 - d + d * vertex.messageQueue[Double].sum
      vertex.setState("prlabel",newLabel)
      if (Math.abs(newLabel-currentLabel)/currentLabel > 0.01) {
        val outEdges = vertex.getOutEdges()
        val outDegree = outEdges.size
        if (outDegree > 0) {
          val toSend = newLabel/outDegree
          outEdges.foreach(edge => {
            edge.send(toSend)
          })
        }
      }
      else {
        vertex.voteToHalt()
      }
    }

  override def returnResults(): (Int, List[(Long,Double)]) = {
    val pageRankings = view.getVertices().map { vertex =>
      val pr = vertex.getState[Double]("prlabel")
      (vertex.ID(), pr)
    }
    val totalV = pageRankings.size
    val topUsers = pageRankings.toList.sortBy(x => x._2)(sortOrdering).take(100)
    (totalV, topUsers)
  }

  override def defineMaxSteps(): Int = 20

  override def extractResults(results: List[(Int, List[(Long,Double)])]): Map[String,Any]  = {
    val totalVert = results.map(x => x._1).sum
    val bestUsers = results.flatMap(x => x._2)
      .sortBy(x => x._2)(sortOrdering)
      .take(100)
      .map(x => s"""{"id":${x._1},"pagerank":${x._2}}""").mkString("[",",","]")
    Map[String,Any]("vertices"->totalVert,"bestusers"->bestUsers)
  }

}