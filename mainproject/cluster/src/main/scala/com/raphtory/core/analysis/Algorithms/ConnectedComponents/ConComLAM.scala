package com.raphtory.core.analysis.Algorithms.ConnectedComponents

import java.util.Date

import com.raphtory.core.analysis.Analyser
import com.raphtory.core.components.AnalysisManager.{LiveAnalysisManager, RangeAnalysisManager, ViewAnalysisManager, WindowRangeAnalysisManager}
import com.raphtory.core.analysis.Algorithms.PageRank.PageRankAnalyser

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ConComLAM(jobID:String,start:Long,end:Long,hop:Long,window:Long) extends WindowRangeAnalysisManager(jobID,start,end,hop,window)  {

  override protected def processResults(): Unit = {

    //println("starting processing")
    val endResults = results.asInstanceOf[ArrayBuffer[mutable.HashMap[Int, Int]]]
    //println("as instance of")
    //endResults.foreach(println(_))
    //println("mapvalues")
    //var results = mutable.HashMap[Int, Int]()
//    endResults.foreach(pmap => {
//      pmap.foreach(f => {results.get(f._1) match {
//        case Some(previousTotal) => results(f._1) = previousTotal + f._2
//        case None => results(f._1) = f._2
//      }})

//    })
    println(s"At ${new Date(timestamp())} with a window of ${windowSize()/1000/3600} hour(s) there were ${endResults.flatten.groupBy(f=> f._1).mapValues(x=> x.map(_._2).sum).size } connected components. The biggest being ${endResults.flatten.groupBy(f=> f._1).mapValues(x=> x.map(_._2).sum).maxBy(_._2)}")
    //println()
    //endResults.flatten.groupBy(f=> f._1).mapValues(x=> x.map(_._2).sum).foreach(f=> println(s"  Group ${f._1} with size ${f._2} "))
    println()
  }


  override protected def defineMaxSteps(): Int = 10
  override protected def generateAnalyzer : Analyser = new ConComAnalyser()
  override protected def processOtherMessages(value: Any) : Unit = {println ("Not handled message" + value.toString)}

  override protected def checkProcessEnd() : Boolean = {
    partitionsHalting()
  }
}
