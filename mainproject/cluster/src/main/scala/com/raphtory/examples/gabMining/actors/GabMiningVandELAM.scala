package com.raphtory.examples.gabMining.actors

import com.raphtory.core.analysis.Analyser
import com.raphtory.core.components.AnalysisManager.LiveAnalysisManager
import com.raphtory.examples.gabMining.analysis.GabMiningVandE

class GabMiningVandELAM extends LiveAnalysisManager{

  override protected def defineMaxSteps(): Int = 1

  override protected def generateAnalyzer: Analyser = new GabMiningVandE()

  override protected def processResults(result: Any): Unit = {

    println("*********INSIDE LAM: " + result)

  }

  override protected def processOtherMessages(value: Any): Unit = ""
}
