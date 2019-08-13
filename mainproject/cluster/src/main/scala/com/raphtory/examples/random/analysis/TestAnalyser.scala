package com.raphtory.examples.random.analysis

import com.raphtory.core.analysis.{Analyser, WorkerID}

class TestAnalyser extends Analyser {

  import akka.actor.ActorContext
  import com.raphtory.core.analysis.GraphRepositoryProxies.GraphProxy

  override def analyse()(implicit workerID:WorkerID): Any = "Test Analysis"

  override def setup()(implicit workerID:WorkerID): Any = ""
}