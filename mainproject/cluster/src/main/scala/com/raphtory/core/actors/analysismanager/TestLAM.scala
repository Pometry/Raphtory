package com.raphtory.core.actors.analysismanager
import com.raphtory.core.analysis._

class TestLAM extends LiveAnalysisManager {
  override protected def defineMaxSteps(): Int = 10

  override protected def generateAnalyzer: Analyser = new TestAnalyser2()

  override protected def processResults(result: Any): Unit = println(result)

  override protected def processOtherMessages(value: Any): Unit = ""

}



