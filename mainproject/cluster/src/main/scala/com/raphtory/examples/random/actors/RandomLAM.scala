package com.raphtory.examples.random.actors

import com.raphtory.core.components.AnalysisManager.LiveAnalysisManager
import com.raphtory.core.analysis._
import com.raphtory.examples.random.analysis.TestAnalyser

class RandomLAM(jobID:String) extends LiveAnalysisManager(jobID) {
  override protected def defineMaxSteps(): Int = 10

  override protected def generateAnalyzer: Analyser = new TestAnalyser()

  override protected def processResults(): Unit = println(results)

  override protected def processOtherMessages(value: Any): Unit = ""

}



