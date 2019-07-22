package com.raphtory.examples.gabMining.analysis
import com.raphtory.core.analysis.{Analyser, GraphRepoProxy, VertexVisitor, Worker}
import akka.actor.ActorContext

import scala.collection.mutable.ArrayBuffer

class GabMiningAnalyser extends Analyser{

  override def analyse()(implicit workerID:Worker): Any = {
    var results = ArrayBuffer[(Int,Int)]()
    var maxInDegree=0
    var maxVertex=0
    proxy.getVerticesSet().foreach(v => {

      val vertex = proxy.getVertex(v)
      var totalEdges: Int = vertex.getIngoingNeighbors.size
      results+=((v, totalEdges))
     // println(" **********Total edges :" + results)
    })

    //results

    for ((a, b) <- results){
      // println("********SUUBBBB LAAAMMM Vertex: "+ a +"Edges : "+ b)
      if(b.toString.toInt>maxInDegree) {
        maxInDegree= b.toString.toInt
        maxVertex=a.toString.toInt
      }
//      else{
//        maxInDegree= b.toString.toInt
//        maxVertex=a.toString.toInt
//      }
    }
    (maxVertex,maxInDegree)

  }

  override def setup()(implicit workerID:Worker): Any = {

  }

}
