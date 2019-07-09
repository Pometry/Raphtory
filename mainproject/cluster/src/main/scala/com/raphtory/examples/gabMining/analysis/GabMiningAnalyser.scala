package com.raphtory.examples.gabMining.analysis
import com.raphtory.core.analysis.{Analyser, GraphRepoProxy, VertexVisitor}
import akka.actor.ActorContext

import scala.collection.mutable.ArrayBuffer

class GabMiningAnalyser extends Analyser{

  override def analyse(): Any = {
    var results = ArrayBuffer[(Int,Int)]()
    proxy.getVerticesSet().foreach(v => {

      val vertex = proxy.getVertex(v)
      var totalEdges: Int = vertex.getIngoingNeighbors.size
      results+=((v, totalEdges))
      println(" **********Total edges :" + results)
    })

    results

  }


  override def setup(): Any = {

  }

}
