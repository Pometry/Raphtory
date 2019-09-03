package com.raphtory.core.analysis.Algorithms

import java.util.Date

import com.raphtory.core.analysis.API.{Analyser, WorkerID}
import com.raphtory.core.model.communication.VertexMessage

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParTrieMap
case class ClusterLabel(value: Int) extends VertexMessage
class ConComAnalyser extends Analyser {


  override def setup()(implicit workerID: WorkerID) = {
    proxy.getVerticesSet().foreach(v => {
      val vertex = proxy.getVertex(v)
      var min = v
      //math.min(v, vertex.getOutgoingNeighbors.union(vertex.getIngoingNeighbors).min)
      val toSend = vertex.getOrSetCompValue("cclabel", min).asInstanceOf[Int]
      vertex.messageAllNeighbours(ClusterLabel(toSend))
    })
  }

  override def analyse()(implicit workerID: WorkerID): Any = {
    val results = ParTrieMap[Int, Int]()
    var verts = Set[Int]()
    println(s"Here !!! $workerID ${proxy.getVerticesSet().size}")
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

  override def defineMaxSteps(): Int = 10
  override def checkProcessEnd(results:ArrayBuffer[Any],oldResults:ArrayBuffer[Any]) : Boolean = {false
    // move to LAM partitionsHalting()
  }
}
