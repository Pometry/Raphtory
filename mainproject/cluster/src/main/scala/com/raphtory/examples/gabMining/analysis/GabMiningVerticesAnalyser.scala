package com.raphtory.examples.gabMining.analysis
import com.raphtory.core.analysis.{Analyser, GraphRepoProxy, VertexVisitor, Worker}
import akka.actor.ActorContext

class GabMiningVerticesAnalyser extends Analyser {
  override def analyse()(implicit workerID:Worker): Any = {

    (proxy.getVerticesSet().size,proxy.getEdgesSet().size)


  }
  override def setup()(implicit workerID:Worker): Any = {

  }
}
