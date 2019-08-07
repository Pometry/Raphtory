package com.raphtory.examples.GenericAlgorithms.ConnectedComponents

import com.raphtory.core.analysis.{Analyser, WorkerID}
import com.raphtory.core.model.communication.VertexMessage

import scala.collection.mutable
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

  override def analyse()(implicit workerID: WorkerID): Any= {
    var results = ParTrieMap[Int, Int]()
    var verts = Set[Int]()
    //println(s"$workerID ${proxy.getVerticesSet().size}")
    for(v <- proxy.getVerticesSet()){
      val vertex = proxy.getVertex(v)
      val queue = vertex.messageQueue.map(_.asInstanceOf[ClusterLabel].value)
      var label = v
      if(queue.nonEmpty)
        label = queue.min
      vertex.messageQueue.clear
      var currentLabel = vertex.getOrSetCompValue("cclabel",v).asInstanceOf[Int]
      if (label < currentLabel) {
        vertex.setCompValue("cclabel", label)
        vertex messageAllNeighbours  (ClusterLabel(label))
        currentLabel = label
      }
      else{
        vertex messageAllNeighbours (ClusterLabel(currentLabel))
        //vertex.voteToHalt()
      }
      results.put(currentLabel, 1+results.getOrElse(currentLabel,0))
      verts+=v
    }
    //if(verts.size!=proxy.getVerticesSet().size)
    //  println(println(s"$workerID ${proxy.getVerticesSet().size} ${verts.size} $verts"))
    results
  }


}
