package com.raphtory.examples.random.analysis

import com.raphtory.core.components.AnalysisManager.LiveAnalysisManager
import com.raphtory.core.analysis.Analyser

class RandomVoidAnalyser(jobID:String) extends LiveAnalysisManager(jobID) {

  override def preStart(): Unit = {}
  override protected def processResults(): Unit = {}
  override protected def defineMaxSteps(): Int = {1}
  override protected def generateAnalyzer : Analyser = null
  override protected def processOtherMessages(value: Any) : Unit = {println ("Not handled message" + value.toString)}
  override protected def checkProcessEnd() : Boolean = true

}