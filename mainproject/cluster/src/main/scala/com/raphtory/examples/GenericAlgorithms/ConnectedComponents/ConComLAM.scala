package com.raphtory.examples.GenericAlgorithms.ConnectedComponents

import com.raphtory.core.analysis.Analyser
import com.raphtory.core.components.AnalysisManager.LiveAnalysisManager
import com.raphtory.examples.GenericAlgorithms.PageRank.PageRankAnalyser

import scala.collection.mutable

class ConComLAM(jobID:String) extends LiveAnalysisManager(jobID)  {

  override protected def processResults(result: Any): Unit = {
    //println("starting processing")
    val endResults = result.asInstanceOf[Vector[mutable.HashMap[Int, Int]]]
    //println("as instance of")
    //endResults.foreach(println(_))
    val results = endResults.flatten
    //println("flattern")
    val results2 = results.groupBy(f=> f._1)
    //println("groupby")
    val results3 = results2.mapValues(x=> x.map(_._2).sum)
    //println("mapvalues")
    //var results = mutable.HashMap[Int, Int]()
//    endResults.foreach(pmap => {
//      pmap.foreach(f => {results.get(f._1) match {
//        case Some(previousTotal) => results(f._1) = previousTotal + f._2
//        case None => results(f._1) = f._2
//      }})

//    })
    results3.foreach(f=> print(f+" "))
    println()
  }


  override protected def defineMaxSteps(): Int = 10
  override protected def generateAnalyzer : Analyser = new ConComAnalyser()
  override protected def processOtherMessages(value: Any) : Unit = {println ("Not handled message" + value.toString)}

  override protected def checkProcessEnd() : Boolean = {
    partitionsHalting()
  }
}
