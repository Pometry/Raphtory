package com.raphtory.examples.gabMining.actors

import com.raphtory.core.components.AnalysisManager.LiveAnalysisManager
import com.raphtory.core.analysis.Analyser
import com.raphtory.examples.gabMining.analysis.GabMiningDistribAnalyser
import com.raphtory.examples.gabMining.utils.writeToFile

import scala.collection.mutable.ArrayBuffer


class GabMiningDistribLAM (jobID:String) extends LiveAnalysisManager (jobID)  {
  val writing=new writeToFile()

  override protected def defineMaxSteps(): Int = 1

  override protected def generateAnalyzer: Analyser = new GabMiningDistribAnalyser()

  override protected def processResults(): Unit = {
    //var finalResults = ArrayBuffer[Int]()

    //println("INSIDE LAM Results: "+ results)
    //println("INSIDE LAM Results: "+results)

    for(indiResult <- results){

      for(inDeg <- indiResult.asInstanceOf[ArrayBuffer[Int]]){

        writing.writeLines("inDegree.csv", inDeg.toString)

      }
    }

    }

  override protected def processOtherMessages(value: Any): Unit = ""
}
