package com.raphtory.examples.gabMining.actors

import com.raphtory.core.analysis.Analyser
import com.raphtory.core.components.AnalysisManager.LiveAnalysisManager
import com.raphtory.examples.gabMining.analysis.GabMiningVerticesAnalyser

class GabMiningVerticesLAM(jobID:String) extends LiveAnalysisManager(jobID) {

  override protected def defineMaxSteps(): Int = 1

  override protected def generateAnalyzer: Analyser = new GabMiningVerticesAnalyser()

  override protected def processResults(result: Any): Unit = {

    val results:Vector[Any] = result.asInstanceOf[(Vector[(Int,Long)])]
    var totalVertices=0

    println("*********INSIDE LAM: " + results)
//    for (a <- results){
//      totalVertices+=a.toString.toInt
//    }
//
//    println("Total vertices: "+ totalVertices )

  }

  override protected def processOtherMessages(value: Any): Unit = ""
}
