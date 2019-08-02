package com.raphtory.examples.gabMining.analysis
import com.raphtory.core.analysis.{Analyser, VertexVisitor, WorkerID}
import akka.actor.ActorContext

import scala.collection.mutable

class GabMiningVerticesAnalyser extends Analyser {
  override def analyse()(implicit workerID:WorkerID): Any = {
    val vertset =mutable.Set[Int]()
    proxy.getVerticesSet().foreach(v =>
      vertset.add(v)
    )
    vertset
  }
  override def setup()(implicit workerID:WorkerID): Any = {

  }
}
