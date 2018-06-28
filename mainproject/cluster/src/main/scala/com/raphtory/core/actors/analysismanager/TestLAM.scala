package com.raphtory.core.actors.analysismanager
import com.raphtory.core.analysis._

class TestLAM extends LiveAnalyser {
  override protected def defineMaxSteps(): Int = 10

  override protected def generateAnalyzer: Analyser = new TestAnalyser()

  override protected def processResults(result: Any): Unit = println(result)

  override protected def processOtherMessages(value: Any): Unit = ""

  override protected def missingCode() = """private class TestAnalyser1 extends Analyser {
                                           |
                                           |import akka.actor.ActorContext
                                           |import com.raphtory.core.storage.controller.GraphRepoProxy
                                           |
                                           |  override implicit var context: ActorContext = _
                                           |  override implicit var managerCount: Int = _
                                           |
                                           |  override def analyse()(implicit proxy: GraphRepoProxy.type, managerCount: Int): Any = "goodbye"
                                           |
                                           |  override def setup()(implicit proxy: GraphRepoProxy.type): Any = ""
                                           |}"""
}

private class TestAnalyser1 extends Analyser {

import akka.actor.ActorContext
import com.raphtory.core.storage.controller.GraphRepoProxy

  override implicit var context: ActorContext = _
  override implicit var managerCount: Int = _

  override def analyse()(implicit proxy: GraphRepoProxy.type, managerCount: Int): Any = "goodbye"

  override def setup()(implicit proxy: GraphRepoProxy.type): Any = ""
}


private class TestAnalyser extends Analyser {

import akka.actor.ActorContext
import com.raphtory.core.storage.controller.GraphRepoProxy

  override implicit var context: ActorContext = _
  override implicit var managerCount: Int = _

  override def analyse()(implicit proxy: GraphRepoProxy.type, managerCount: Int): Any = "hello"

  override def setup()(implicit proxy: GraphRepoProxy.type): Any = ""
}
