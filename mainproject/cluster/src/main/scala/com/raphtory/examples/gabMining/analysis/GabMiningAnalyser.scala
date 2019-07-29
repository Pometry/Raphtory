package com.raphtory.examples.gabMining.analysis
import com.raphtory.core.analysis.{Analyser, GraphRepoProxy, VertexVisitor, WorkerID}
import akka.actor.ActorContext

import scala.collection.mutable.ArrayBuffer

class GabMiningAnalyser extends Analyser{

  override def analyse()(implicit workerID:WorkerID): Any = {
    var results = ArrayBuffer[(Int,Int)]()
    var maxInDegree=0
    var maxVertex=0
    proxy.getVerticesSet().foreach(v => {

      val vertex = proxy.getVertex(v)
      val totalEdges: Int = vertex.getIngoingNeighbors.size
      results+=((v, totalEdges))
      println(" **********Total edges :" + results)

    })


    for ((vertex, inDegree) <- results){
      // println("********SUUBBBB LAAAMMM Vertex: "+ vertex +"Edges : "+ inDegree)
      if(inDegree.toString.toInt>maxInDegree) {
        maxInDegree= inDegree.toString.toInt
        maxVertex=vertex.toString.toInt
      }

    }
    (maxVertex,maxInDegree)

  }

  override def setup()(implicit workerID:WorkerID): Any = {

  }

}
