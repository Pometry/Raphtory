package com.raphtory.examples.GenericAlgorithms.ConnectedComponents

import com.raphtory.core.analysis.{Analyser, WorkerID}
import com.raphtory.core.model.communication.VertexMessage

import scala.collection.mutable

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
    var results = mutable.HashMap[Int, Int]()
    ///println(s"Worker $workerID "+proxy.getVerticesSet().size)
    proxy.getVerticesSet().foreach(v => {

      if(workerID.ID==1)
        println(s"Trying to get vertex ${v}")
      val vertex = proxy.getVertex(v)
      if(workerID.ID==1)
        println(s"Trying to get message queue")
      val queue = vertex.messageQueue.map(_.asInstanceOf[ClusterLabel].value)
      var label = v
      if(queue.nonEmpty)
        label = queue.min
      vertex.messageQueue.clear
      //while (vertex moreMessages)
       // label = math.min(label, vertex.nextMessage().asInstanceOf[ClusterLabel].value)
      var currentLabel = vertex.getOrSetCompValue("cclabel",v).asInstanceOf[Int]
      if (label < currentLabel) {
        vertex.setCompValue("cclabel", label)
        vertex messageAllOutgoingNeighbors (ClusterLabel(label))
        currentLabel = label
      }
      else{
        vertex messageAllOutgoingNeighbors (ClusterLabel(label))
        //vertex.voteToHalt()
      }
      results.put(v, 1+results.getOrElse(v,0))
//      results.get(currentLabel) match {
//        case Some(currentCount) => results(currentLabel) = currentCount +1
//        case None => results(currentLabel) = 1
//      }
      if(workerID.ID==1)
        println(s"Finished vertex ${v}")
    })
    println(s"Worker $workerID")
    results
  }


}
