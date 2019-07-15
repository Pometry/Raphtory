package com.raphtory.examples.gabMining.analysis
import com.raphtory.core.analysis.{Analyser, GraphRepoProxy, VertexVisitor, WorkerID}
import akka.actor.ActorContext

class GabMiningVerticesAnalyser extends Analyser {
  override def analyse()(implicit workerID:WorkerID): Any = {

   // (proxy.getVerticesSet().size,proxy.getEdgesSet().size)


  }
  override def setup()(implicit workerID:WorkerID): Any = {

  }
}
