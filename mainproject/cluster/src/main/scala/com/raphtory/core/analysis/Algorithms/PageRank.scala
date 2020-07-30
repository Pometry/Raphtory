package com.raphtory.core.analysis.Algorithms

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.utils.Utils

import scala.collection.mutable.ArrayBuffer

class PageRank(args:Array[String]) extends Analyser(args) {
  object sortOrdering extends Ordering[Double] {
    def compare(key1: Double, key2: Double) = key2.compareTo(key1)
  }

  override def setup(): Unit =
    view.getVertices().foreach { vertex =>
      val outDegree = vertex.getOutEdges.size
      //math.min(v, vertex.getOutgoingNeighbors.union(vertex.getIngoingNeighbors).min)
      if (outDegree > 0) {
        val toSend = 1.0/outDegree
        vertex.setState("prlabel",toSend)
        vertex.messageAllOutgoingNeighbors(toSend)
      } else {
        vertex.setState("prlabel",0.0)
      }
    }

  override def analyse(): Unit =
    view.getMessagedVertices().foreach {vertex =>
      val currentLabel = vertex.getState[Double]("prlabel")
      val newLabel = vertex.messageQueue[Double].sum
      vertex.setState("prlabel",newLabel)
      if (Math.abs(newLabel-currentLabel)/currentLabel > 0.01) {
        val outDegree = vertex.getOutEdges.size
        if (outDegree > 0) {
          val toSend = newLabel/outDegree
          vertex.messageAllOutgoingNeighbors(toSend)
        }
      }
      else {
        vertex.voteToHalt()
      }
    }

  override def returnResults(): Any = {
    val pageRankings = view.getVertices().map { vertex =>
      val pr = vertex.getState[Double]("prlabel")
      (vertex.ID, pr)
    }
    val totalV = pageRankings.size
    val topUsers = pageRankings.toArray.sortBy(x => x._2)(sortOrdering).take(10)
    (totalV, topUsers)
  }

  override def defineMaxSteps(): Int = 10

  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[(Int, Array[(Long,Double)])]]
    val totalVert = endResults.map(x => x._1).sum
    val bestUsers = endResults
      .map(x => x._2)
      .flatten
      .sortBy(x => x._2)(sortOrdering)
      .take(10)
      .map(x => s"""{"id":${x._1},"pagerank":${x._2}}""").mkString("[",",","]")
    val text = s"""{"time":$timeStamp,"vertices":$totalVert,"bestusers":$bestUsers,"viewTime":$viewCompleteTime}"""
    println(text)
  }
}