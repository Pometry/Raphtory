package com.raphtory.algorithms

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.utils.Utils

import scala.collection.mutable.ArrayBuffer

class GabWeightedPageRank(args:Array[String]) extends Analyser(args) {
  object sortOrdering extends Ordering[Double] {
    def compare(key1: Double, key2: Double) = key2.compareTo(key1)
  }

  // damping factor, (1-d) is restart probability
  val d = 0.85

  val toWatch = Set(31,4987,1981,709,1175,18992,491,5196,1759,4555).map(_.toLong)

  override def setup(): Unit =
    view.getVertices().foreach { vertex =>
      val outEdges = vertex.getOutEdges
      val outDegree = outEdges.size

      if (outDegree > 0) {
        val toSend = 1.0/outEdges.map(e=> e.getHistory().size).sum
        vertex.setState("prlabel",toSend)
        outEdges.foreach(edge => {
          val modifyer = edge.getHistory().size
          edge.send(toSend*modifyer)
        })
      } else {
        vertex.setState("prlabel",0.0)
      }
    }

  override def analyse(): Unit =
    view.getMessagedVertices().foreach {vertex =>
      val currentLabel = vertex.getState[Double]("prlabel")
      val messages = vertex.messageQueue[Double]
      val newLabel = (1-d) + d * messages.sum
      vertex.setState("prlabel",newLabel)
      if (Math.abs(newLabel-currentLabel)/currentLabel > 0.01) {
        val outEdges = vertex.getOutEdges
        val outDegree = outEdges.size
        if (outDegree > 0) {
          val toSend = newLabel/outEdges.map(e=> e.getHistory().size).sum
          outEdges.foreach(edge => {
            val modifyer = edge.getHistory().size
            edge.send(toSend*modifyer)
          })
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
      .filter(inFilter)
      .map(x => s"""{"id":${x._1},"pagerank":${x._2}}""").mkString("[",",","]")
    val text = s"""{"time":$timeStamp,"vertices":$totalVert,"bestusers":$bestUsers,"viewTime":$viewCompleteTime}"""
    println(text)
  }

  override def processWindowResults(results: ArrayBuffer[Any], timestamp: Long, windowSize: Long,
                                    viewCompleteTime: Long ):
  Unit = {
    var output_folder = System.getenv().getOrDefault("OUTPUT_FOLDER", "/app").trim
    var output_file = output_folder + "/" + System.getenv().getOrDefault("OUTPUT_FILE","WeightedPageRank.json").trim
    val endResults = results.asInstanceOf[ArrayBuffer[(Int, Array[(Long,Double)])]]
    val totalVert = endResults.map(x => x._1).sum
    val bestUsers = endResults
      .map(x => x._2)
      .flatten
      .filter(inFilter)
      .map(x => s"""{"id":${x._1},"pagerank":${x._2}}""").mkString("[",",","]")
    val text =
      s"""{"time":$timestamp,"windowsize":$windowSize,"vertices":$totalVert,"bestusers":$bestUsers,"viewTime":$viewCompleteTime}"""
    Utils.writeLines(output_file, text, "{\"views\":[")
    println(text)
    //publishData(text)
  }

  def inFilter(item: (Long, Double)):
  Boolean = {
    toWatch.contains(item._1)
  }

}