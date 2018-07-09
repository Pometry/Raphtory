package com.raphtory.examples.random.actors

import com.raphtory.core.actors.analysismanager.LiveAnalyser
import com.raphtory.core.analysis.Analyser
import com.raphtory.examples.gab.analysis.GabMostUsedTopics

class RandomVoidAnalyser extends LiveAnalyser {

  override def preStart(): Unit = {}
  override protected def processResults(result: Any): Unit = {}
  override protected def defineMaxSteps(): Unit = {}
  override protected def generateAnalyzer : Analyser = null
  override protected def processOtherMessages(value: Any) : Unit = {println ("Not handled message" + value.toString)}
  override protected def checkProcessEnd() : Boolean = true

}