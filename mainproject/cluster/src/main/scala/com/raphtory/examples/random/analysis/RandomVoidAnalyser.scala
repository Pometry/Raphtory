package com.raphtory.examples.random.analysis

import com.raphtory.core.actors.analysismanager.LiveAnalysisManager
import com.raphtory.core.analysis.Analyser

class RandomVoidAnalyser extends LiveAnalysisManager {

  override def preStart(): Unit = {}
  override protected def processResults(result: Any): Unit = {}
  override protected def defineMaxSteps(): Int = {1}
  override protected def generateAnalyzer : Analyser = null
  override protected def processOtherMessages(value: Any) : Unit = {println ("Not handled message" + value.toString)}
  override protected def checkProcessEnd() : Boolean = true

}