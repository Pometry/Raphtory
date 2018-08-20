package com.raphtory.examples.random.actors

import com.raphtory.core.actors.analysismanager.LiveAnalysisManager
import com.raphtory.core.analysis._
import com.raphtory.examples.random.analysis.TestAnalyser

class RandomLAM extends LiveAnalysisManager {
  override protected def defineMaxSteps(): Int = 10

  override protected def generateAnalyzer: Analyser = new TestAnalyser()

  override protected def processResults(result: Any): Unit = println(result)

  override protected def processOtherMessages(value: Any): Unit = ""

}



