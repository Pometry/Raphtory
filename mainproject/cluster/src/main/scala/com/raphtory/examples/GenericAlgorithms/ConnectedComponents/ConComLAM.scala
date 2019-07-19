package com.raphtory.examples.GenericAlgorithms.ConnectedComponents

import com.raphtory.core.analysis.Analyser
import com.raphtory.core.components.AnalysisManager.LiveAnalysisManager
import com.raphtory.examples.GenericAlgorithms.PageRank.PageRankAnalyser

import scala.collection.mutable

class ConComLAM(jobID:String) extends LiveAnalysisManager(jobID)  {

  override protected def processResults(result: Any): Unit = {
    val endResults = result.asInstanceOf[Vector[mutable.HashMap[Int, Int]]]
    var results = mutable.HashMap[Int, Int]()
    //endResults.foreach(f => )
  }
  override protected def defineMaxSteps(): Int = {
    1000
  }

  override protected def generateAnalyzer : Analyser = new ConComAnalyser()
  override protected def processOtherMessages(value: Any) : Unit = {println ("Not handled message" + value.toString)}

  override protected def checkProcessEnd() : Boolean = {
    false
  }
}
