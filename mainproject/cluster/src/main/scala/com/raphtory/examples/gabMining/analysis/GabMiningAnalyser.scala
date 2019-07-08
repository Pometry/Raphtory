package com.raphtory.examples.gabMining.analysis
import com.raphtory.core.analysis.{Analyser, GraphRepoProxy, VertexVisitor}
import akka.actor.ActorContext

class GabMiningAnalyser extends Analyser{

  override def analyse(): Any = {
    proxy.getVerticesSet().foreach(v => {

      val vertex = proxy.getVertex(v)
      var totalEdges: Int = vertex.getIngoingNeighbors.size
      var results = (v, totalEdges)
      println("**********Total edges :" + results)
      return results

    })
  }


  override def setup(): Any = {

  }

}
