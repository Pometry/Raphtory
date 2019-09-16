package com.raphtory.core.analysis.Algorithms

import java.util.Date

import com.raphtory.core.analysis.API.{Analyser, WorkerID}
import com.raphtory.core.model.communication.VertexMessage
import com.raphtory.core.utils.Utils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParTrieMap
case class ClusterLabel(value: Int) extends VertexMessage
class ConnectedComponents extends Analyser {


  override def setup() = {
    proxy.getVerticesSet().foreach(v => {
      val vertex = proxy.getVertex(v)
      var min = v
      //math.min(v, vertex.getOutgoingNeighbors.union(vertex.getIngoingNeighbors).min)
      val toSend = vertex.getOrSetCompValue("cclabel", min).asInstanceOf[Int]
      vertex.messageAllNeighbours(ClusterLabel(toSend))
    })
  }

  override def analyse(): Any = {
    val results = ParTrieMap[Int, Int]()
    var verts = Set[Int]()
    for (v <- proxy.getVerticesSet()) {
      val vertex = proxy.getVertex(v)
      val queue = vertex.messageQueue.map(_.asInstanceOf[ClusterLabel].value)
      var label = v
      if (queue.nonEmpty)
        label = queue.min
      vertex.messageQueue.clear
      var currentLabel = vertex.getOrSetCompValue("cclabel", v).asInstanceOf[Int]
      if (label < currentLabel) {
        vertex.setCompValue("cclabel", label)
        vertex messageAllNeighbours (ClusterLabel(label))
        currentLabel = label
      }
      else {
        vertex messageAllNeighbours (ClusterLabel(currentLabel))
        //vertex.voteToHalt()
      }
      results.put(currentLabel, 1 + results.getOrElse(currentLabel, 0))
      verts += v
    }
    results
  }
  override def processResults(results: ArrayBuffer[Any], oldResults: ArrayBuffer[Any]): Unit = {}

  override def processViewResults(results: ArrayBuffer[Any], oldResults: ArrayBuffer[Any], timestamp: Long): Unit = {}

  override def processWindowResults(results: ArrayBuffer[Any], oldResults: ArrayBuffer[Any],timestamp:Long,windowSize:Long): Unit = {
  val endResults = results.asInstanceOf[ArrayBuffer[mutable.HashMap[Int, Int]]]
  println(s"At ${new Date(timestamp)} with a window of ${windowSize / 1000 / 3600} hour(s) there were ${endResults.flatten.groupBy(f => f._1).mapValues(x => x.map(_._2).sum).size} connected components. The biggest being ${endResults.flatten.groupBy(f => f._1).mapValues(x => x.map(_._2).sum).maxBy(_._2)}")
  println()
  }

  override def processBatchWindowResults(results: ArrayBuffer[Any], oldResults: ArrayBuffer[Any], timestamp: Long, windowSet: Array[Long]): Unit = {
    var output_file = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/app/defout.csv").trim
    val endResults = results.asInstanceOf[ArrayBuffer[ArrayBuffer[mutable.HashMap[Int, Int]]]]
    for(i <- endResults.indices){
      val window = endResults(i)
      val windowSize = windowSet(i)
      val biggest = window.flatten.groupBy(f => f._1).mapValues(x => x.map(_._2).sum).maxBy(_._2)
      val total = window.flatten.groupBy(f => f._1).mapValues(x => x.map(_._2).sum).size
      println(s"$timestamp $biggest $total")
      try {
        val text =s"{\"time\":$timestamp,\"windowsize\":${windowSet(i)},\"biggest\":$biggest,\"total\":$total},"
        Utils.writeLines(output_file,text,"{\"views:[\"")
        //println(s"At ${new Date(timestamp)} with a window of ${windowSize / 3600000} hour(s) there were ${} connected components. The biggest being ${}")
      }catch {
        case e:UnsupportedOperationException => println("empty.maxby")
      }
    }

  }

  override def defineMaxSteps(): Int = 10
  override def checkProcessEnd(results:ArrayBuffer[Any],oldResults:ArrayBuffer[Any]) : Boolean = {false
    // move to LAM partitionsHalting()
  }
}
