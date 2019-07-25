package com.raphtory.examples.GenericAlgorithms.ConnectedComponents

import com.raphtory.core.analysis.{Analyser, WorkerID}
import com.raphtory.core.model.communication.VertexMessage

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

class ConComAnalyser extends Analyser {

  case class ClusterLabel(value: Int) extends VertexMessage

  override def setup()(implicit workerID: WorkerID) = {
    proxy.getVerticesSet().foreach(v => {
      val vertex = proxy.getVertex(v)
      var min = v
      //math.min(v, vertex.getOutgoingNeighbors.union(vertex.getIngoingNeighbors).min)
      val toSend = vertex.getOrSetCompValue("cclabel", min).asInstanceOf[Int]
      vertex.messageAllNeighbours(ClusterLabel(toSend))
    })
  }

  override def analyse()(implicit workerID: WorkerID): Any= {
    var results = ParTrieMap[Int, Int]()
    proxy.getVerticesSet().foreach(v => {

      val vertex = proxy.getVertex(v)
      val queue = vertex.messageQueue.map(_.asInstanceOf[ClusterLabel].value)
      var label = v
      if(queue.nonEmpty)
        label = queue.min
      vertex.messageQueue.clear
      var currentLabel = vertex.getOrSetCompValue("cclabel",v).asInstanceOf[Int]
      if (label < currentLabel) {
        vertex.setCompValue("cclabel", label)
        vertex messageAllOutgoingNeighbors (ClusterLabel(label))
        currentLabel = label
      }
      else{
        vertex messageAllOutgoingNeighbors (ClusterLabel(currentLabel))
        //vertex.voteToHalt()
      }
      results.put(currentLabel, 1+results.getOrElse(currentLabel,0))

    })
    results
  }


}
