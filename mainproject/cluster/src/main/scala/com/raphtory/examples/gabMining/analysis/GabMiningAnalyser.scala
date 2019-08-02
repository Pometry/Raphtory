package com.raphtory.examples.gabMining.analysis
import com.raphtory.core.analysis.{Analyser, GraphRepoProxy, VertexVisitor, WorkerID}
import akka.actor.ActorContext

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParTrieMap

class GabMiningAnalyser extends Analyser{

  override def setup()(implicit workerID: WorkerID) = {
    proxy.getVerticesSet().foreach(v => {
      val vertex = proxy.getVertex(v)
      //var min = v
      //math.min(v, vertex.getOutgoingNeighbors.union(vertex.getIngoingNeighbors).min)
      vertex.getOrSetCompValue("inDegree", 0)
    })
  }

  override def analyse()(implicit workerID:WorkerID): Any = {
    var results = ParTrieMap[Int, Int]()

    for(v <- proxy.getVerticesSet()){
      val vertex = proxy.getVertex(v)
      val inDegree= vertex.getIngoingNeighbors.size
      val currentInDegree= vertex.getOrSetCompValue("inDegree",v).asInstanceOf[Int]

      if(inDegree>currentInDegree){

        vertex.getOrSetCompValue("inDegree", inDegree)

        //results+=((v,inDegree))
        results.put(v, inDegree)

      }

    }
    results
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
