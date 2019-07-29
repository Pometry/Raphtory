package com.raphtory.examples.gabMining.analysis
import com.raphtory.core.analysis.{Analyser, GraphRepoProxy, VertexVisitor, WorkerID}
import akka.actor.ActorContext
import monix.execution.atomic.AtomicLong

//<<<<<<< HEAD:mainproject/cluster/src/main/scala/com/raphtory/examples/gabMining/analysis/GabMiningDensityAnalyser.scala
class GabMiningDensityAnalyser extends Analyser {
  override def analyse()(implicit workerID:WorkerID): Any = {

    (proxy.getVerticesSet().size,proxy.getEdgesSet().size)

  }
  override def setup()(implicit workerID:WorkerID): Any = {

  }
}
