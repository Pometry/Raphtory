package com.raphtory.examples.GenericAlgorithms.ConnectedComponents

import com.raphtory.core.analysis.Analyser
import com.raphtory.core.components.AnalysisManager.LiveAnalysisManager
import com.raphtory.examples.GenericAlgorithms.PageRank.PageRankAnalyser

import scala.collection.mutable

class ConComLAM(jobID:String) extends LiveAnalysisManager(jobID)  {

  override protected def processResults(result: Any): Unit = {
    val endResults = result.asInstanceOf[Vector[mutable.HashMap[Int, Int]]]
    var results = mutable.HashMap[Int, Int]()
    endResults.foreach(pmap => {
      pmap.foreach(f => {results.get(f._1) match {
        case Some(previousTotal) => results(f._1) = previousTotal + f._2
        case None => results(f._1) = f._2
      }})

    })
    results.foreach(f=> if(f._2>1) print(f+" "))
  }
  override protected def defineMaxSteps(): Int = {
    100
  }

  override protected def generateAnalyzer : Analyser = new ConComAnalyser()
  override protected def processOtherMessages(value: Any) : Unit = {println ("Not handled message" + value.toString)}

  override protected def checkProcessEnd() : Boolean = {
    partitionsHalting()
  }
}
