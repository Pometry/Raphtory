package com.raphtory.examples.gabMining.actors

import com.raphtory.core.analysis.Analyser
import com.raphtory.core.components.AnalysisManager.LiveAnalysisManager
import com.raphtory.examples.gabMining.analysis.GabMiningVerticesAnalyser

class GabMiningVerticesLAM(jobID:String) extends LiveAnalysisManager(jobID) {

  override protected def defineMaxSteps(): Int = 2

  override protected def generateAnalyzer: Analyser = new GabMiningVerticesAnalyser()

  override protected def processResults(): Unit = {
    var totalVertices=0
    println("*********INSIDE LAM: " + results.asInstanceOf[(Vector[Set[Int]])].flatten)
  }

  override protected def processOtherMessages(value: Any): Unit = ""
}
