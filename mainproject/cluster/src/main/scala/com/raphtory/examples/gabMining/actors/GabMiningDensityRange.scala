package com.raphtory.examples.gabMining.actors

import java.util.Date

import com.raphtory.core.analysis.Analyser
import com.raphtory.core.components.AnalysisManager.RangeAnalysisManager
import com.raphtory.examples.gabMining.analysis.GabMiningDensityAnalyser
import com.raphtory.examples.gabMining.utils.writeToFile

import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat


class GabMiningDensityRange (jobID:String,start:Long,end:Long,jump:Long)extends RangeAnalysisManager (jobID,start,end,jump){
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

    for (verticesAndEdges <- allResults){
      totalVertices+=verticesAndEdges._1
      totalEdges+=verticesAndEdges._2

    }
    val density : Double= (totalEdges.toDouble/(totalVertices.toDouble*(totalVertices.toDouble-1)))
    //println(f"Total vertices: "+ totalVertices + " Total edges: "+ totalEdges + " Density: "+density)
    println (s"The density at ${new Date(timestamp())} is : "+ density)
    val currentDate=new Date(timestamp())
    val formattedDate = outputFormat.format(inputFormat.parse(currentDate.toString))
    val text= formattedDate + " "+ totalVertices + ","+ totalEdges + ","+density
    writing.writeLines("Density3.csv",text)

  }

  override protected def processOtherMessages(value: Any): Unit = ""

}
