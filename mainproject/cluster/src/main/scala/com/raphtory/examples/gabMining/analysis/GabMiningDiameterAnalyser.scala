package com.raphtory.examples.gabMining.analysis
import com.raphtory.core.analysis.{Analyser, WorkerID}
import com.raphtory.core.model.communication.VertexMessage

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParTrieMap

class GabMiningDiameterAnalyser extends Analyser{

  case class ClusterLabel(value: Int) extends VertexMessage

  override def setup()(implicit workerID:WorkerID): Any = {

    proxy.getVerticesSet().foreach(v => {
      val vertex = proxy.getVertex(v)
      val id = v
      //   println("INSIDE ANALYSER TO SEEEND"+toSend)
      if (!vertex.getOutgoingNeighbors.isEmpty)
        vertex.getOrSetCompValue("messageSent", true).asInstanceOf[Boolean]
      else
        //vertex.voteToHalt()
        vertex.getOrSetCompValue("messageSent", false).asInstanceOf[Boolean]

      vertex.messageAllOutgoingNeighbors(ClusterLabel(id))
    })

  }
  override def analyse()(implicit workerID:WorkerID): Any = {
    //println("SEND HISTORY:"+history)
    //  var results = ParTrieMap[Int, Int]()
    //var verts = Set[Int]()
    //println(s"$workerID ${proxy.getVerticesSet().size}")
    for(v <- proxy.getVerticesSet()){

      val vertex = proxy.getVertex(v)
      val messageSent= vertex.getCompValue("messageSent").asInstanceOf[Boolean]

      val queue = vertex.messageQueue.map(_.asInstanceOf[ClusterLabel].value)

      println("THIS IS QUEUE of v:"+v+" "+queue)
      println("THIS IS THE INFO of v: "+v+" "+ vertex.getCompValue("messageSent"))
      if(queue.nonEmpty){
        for(item<-queue){
          if (!messageSent) {
            vertex.messageAllOutgoingNeighbors(ClusterLabel(item))

          }
        }
        vertex.messageQueue.clear
      }

//      else{
//        vertex.voteToHalt()
//      }



    }


  }


}
//
//
//for(item<-queue){
//  if (!history.contains(item)) {
//  history+=item
//}
//}


