package com.raphtory.examples.gabMining.actors

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Date

import com.raphtory.core.analysis.Analyser
import com.raphtory.core.components.AnalysisManager.LiveAnalysisManager
import com.raphtory.examples.gabMining.analysis.GabMiningDensityAnalyser
import com.raphtory.examples.gabMining.utils.writeToFile

import scala.collection.mutable.ArrayBuffer

class GabMiningDensityLAM(jobID: String) extends LiveAnalysisManager(jobID){
  val writing=new writeToFile()
  //Wed Aug 10 04:59:06 BST 2016
  val inputFormat = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
  val outputFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
  override protected def defineMaxSteps(): Int = 1

  override protected def generateAnalyzer: Analyser = new GabMiningDensityAnalyser()

  override protected def processResults(): Unit = {

    var totalVertices=0
    var totalEdges=0

    var allResults=results.asInstanceOf[ArrayBuffer[(Int,Int)]]
    //  println("WHAT CAME FROM ANALYSER: "+allResults)
    for (verticesAndEdges <- allResults){
      totalVertices+=verticesAndEdges._1
      totalEdges+=verticesAndEdges._2

    }
    val density : Float= (totalEdges.toFloat/(totalVertices.toFloat*(totalVertices.toFloat-1)))
    //println(f"Total vertices: "+ totalVertices + " Total edges: "+ totalEdges + " Density: "+density)
    val currentDate=LocalDateTime.now()
   // val formattedDate = outputFormat.format(inputFormat.parse(currentDate.toString))
    val density2=new java.math.BigDecimal(density).toPlainString
    println (s"The density at ${currentDate} is : "+ density2)
    //val text= formattedDate + " "+ totalVertices + ","+ totalEdges + ","+density2
  //  writing.writeLines("results/densityWindow.csv",text)


  }

  override protected def processOtherMessages(value: Any): Unit = ""
}
