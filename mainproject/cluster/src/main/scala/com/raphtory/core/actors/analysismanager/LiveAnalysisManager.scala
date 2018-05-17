package com.raphtory.core.actors.analysismanager

import com.raphtory.examples.gab.analysis.GabPageRank
import com.raphtory.core.analysis.Analyser

class LiveAnalysisManager() extends LiveAnalyser {
  // !!!Not used look at GabLiveAnalysisManager for Gab example!!!
  private val B       : Int   = 100 // TODO set
  private val epsilon : Float = 0.01F // TODO set
  private val delta1  : Float = 1F

  override protected def processResults(result: Any): Unit = println(result)
  override protected def defineMaxSteps(): Unit = steps =  (B * Math.log(getNetworkSize/epsilon)).round
  override protected def generateAnalyzer : Analyser = new GabPageRank(getNetworkSize, epsilon, delta1)
  override protected def processOtherMessages(value: Any) : Unit = {}
}

