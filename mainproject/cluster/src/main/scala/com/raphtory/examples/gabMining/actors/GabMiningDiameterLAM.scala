package com.raphtory.examples.gabMining.actors
import com.raphtory.examples.gabMining.analysis.GabMiningDiameterAnalyser
import com.raphtory.core.components.AnalysisManager.LiveAnalysisManager
import com.raphtory.core.analysis.Analyser

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class GabMiningDiameterLAM (jobID:String) extends LiveAnalysisManager (jobID){
  override protected def defineMaxSteps(): Int = 3

  override protected def generateAnalyzer: Analyser = new GabMiningDiameterAnalyser()

  override protected def processResults(): Unit = {
println(results)
    //println("starting processing")
    //val endResults = results.asInstanceOf[ArrayBuffer[mutable.HashMap[Int, Int]]]
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
   // println(endResults.flatten.groupBy(f=> f._1).mapValues(x=> x.map(_._2).sum))//.foreach(f=> print(f+" "))
    println()
  }



  override protected def processOtherMessages(value: Any): Unit = ""

  override protected def checkProcessEnd() : Boolean = {
    partitionsHalting()
  }

  }




