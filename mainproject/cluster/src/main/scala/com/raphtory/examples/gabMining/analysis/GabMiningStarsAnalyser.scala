package com.raphtory.examples.gabMining.analysis
import com.raphtory.core.analysis.{Analyser, VertexVisitor, WorkerID}
import akka.actor.ActorContext

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParTrieMap


//A star is about to find the maximum in degree and the node that has it at a given time . For this we need to
//parse the vertices that currently exist in the system and get the number of their ingoingNeighbours and we
//sent to the analyser only the vertex  that had the maximum in-degree value. We sent it as a key value pair to
// the analyser.
//
class GabMiningStarsAnalyser extends Analyser{

  override def analyse()(implicit workerID:WorkerID): Any = {
    val results = ParTrieMap[Int, Int]()

    for(v <- proxy.getVerticesSet()){
      val vertex = proxy.getVertex(v)
      val inDegree= vertex.getIngoingNeighbors.size
      //println("CHeck this out:"+v.toInt+" "+ inDegree.toInt)
        results.put(v.toInt, inDegree.toInt)

    }
    if (results.nonEmpty) {
      val max = results.maxBy(_._2)
      (max._1,max._2)
    }

  }

  override def setup()(implicit workerID: WorkerID) = {

  }

//class GabMiningAnalyser extends Analyser{
//
//  override def analyse()(implicit workerID:WorkerID): Any = {
//    var results = ArrayBuffer[(Int,Int)]()
//    var maxInDegree=0
//    var maxVertex=0
//
//    println("********************************MAx In Degree: "+maxInDegree)
//    proxy.getVerticesSet().foreach(v => {
//
//      val vertex = proxy.getVertex(v)
//      var totalEdges: Int = vertex.getIngoingNeighbors.size
//      results+=((v, totalEdges))
//     // println(" **********Total edges :" + results)
//    })
//
//    for ((vertex, inDegree) <- results){
//       println("********SUUBBBB LAAAMMM Vertex: "+ vertex +"Edges : "+ inDegree)
//      if(inDegree.toString.toInt>maxInDegree) {
//        maxInDegree= inDegree.toString.toInt
//        maxVertex=vertex.toString.toInt
//      }
//
//    }
//    (maxVertex,maxInDegree)
//
//  }

//  override def setup()(implicit workerID:WorkerID): Any = {
//
//  }

}
