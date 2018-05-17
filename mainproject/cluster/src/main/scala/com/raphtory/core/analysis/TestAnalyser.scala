package com.raphtory.core.analysis

import akka.actor.ActorContext
import com.raphtory.core.storage.controller.GraphRepoProxy

class TestAnalyser extends Analyser {
  override implicit var context: ActorContext = _
  override implicit var managerCount: Int = _

  override def analyse()(implicit proxy: GraphRepoProxy.type, managerCount: Int): Any = "hello"

  override def setup()(implicit proxy: GraphRepoProxy.type): Any = ???
}
