package com.raphtory.examples.random.analysis

import com.raphtory.core.analysis.Analyser

class TestAnalyser extends Analyser {

  import akka.actor.ActorContext
  import com.raphtory.core.analysis.GraphRepoProxy

  override def analyse(): Any = "Test Analysis"

  override def setup(): Any = ""
}