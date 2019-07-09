package com.raphtory.examples.random.analysis

import com.raphtory.core.analysis.{Analyser, Worker}

class TestAnalyser extends Analyser {

  import akka.actor.ActorContext
  import com.raphtory.core.analysis.GraphRepoProxy

  override def analyse()(implicit workerID:Worker): Any = "Test Analysis"

  override def setup()(implicit workerID:Worker): Any = ""
}