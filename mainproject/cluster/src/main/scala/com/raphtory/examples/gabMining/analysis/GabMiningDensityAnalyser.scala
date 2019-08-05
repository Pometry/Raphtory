package com.raphtory.examples.gabMining.analysis
import com.raphtory.core.analysis.{Analyser, GraphRepoProxy, VertexVisitor, WorkerID}
import akka.actor.ActorContext
import monix.execution.atomic.AtomicLong


class GabMiningDensityAnalyser extends Analyser {
  override def analyse()(implicit workerID:WorkerID): Any = {

   // println("Here :"+proxy.getVerticesSet().size+" "+proxy.getEdgesSet().size)


    (proxy.getVerticesSet().size,proxy.getEdgesSet().size)


  }
  override def setup()(implicit workerID:WorkerID): Any = {

  }
}
