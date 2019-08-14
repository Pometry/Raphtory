package com.raphtory.examples.gabMining.actors

import java.util.Date

import com.raphtory.core.analysis.Analyser
import com.raphtory.core.components.AnalysisManager.RangeAnalysisManager
import com.raphtory.examples.gabMining.analysis.GabMiningDensityAnalyser
import com.raphtory.examples.gabMining.utils.writeToFile

import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat
import java.time.LocalDateTime


class GabMiningDensityRange (jobID:String,start:Long,end:Long,jump:Long)extends RangeAnalysisManager (jobID,start,end,jump){

  val output_file = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/app/defout.csv").trim
  val writing=new writeToFile()
  //Wed Aug 10 04:59:06 BST 2016
  val inputFormat = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
  val outputFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
  writing.writeLines(output_file,"Time,Date,TotalVertices,TotalEdges,Density")
//  val writing=new writeToFile()
//  //Wed Aug 10 04:59:06 BST 2016
//  val inputFormat = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
//  val outputFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
//  writing.writeLines("results/Density/densityRange.csv","Time,Date,TotalVertices,TotalEdges,Density")

  override protected def defineMaxSteps(): Int = 1

  override protected def generateAnalyzer: Analyser = new GabMiningDensityAnalyser()

  override protected def processResults(): Unit = {
    var totalVertices=0
    var totalEdges=0
    var density2="0"

    var allResults=results.asInstanceOf[ArrayBuffer[(Int,Int)]]
    //  println("WHAT CAME FROM ANALYSER: "+allResults)
    for (verticesAndEdges <- allResults){
      totalVertices+=verticesAndEdges._1
      totalEdges+=verticesAndEdges._2

    }
    // println("Total vertices: "+ totalVertices+" Total Edges: "+ totalEdges)


    if(totalVertices<2 && totalEdges==0){
      density2="0"
    }
    if(totalVertices>=2 && totalEdges>=0){
      val density : Float= (totalEdges.toFloat/(totalVertices.toFloat*(totalVertices.toFloat-1)))
      density2 = new java.math.BigDecimal(density).toPlainString
    }

    //println(f"Total vertices: "+ totalVertices + " Total edges: "+ totalEdges + " Density: "+density)
    val currentDate=new Date(timestamp())
    val formattedDate = outputFormat.format(inputFormat.parse(currentDate.toString))
    val time = inputFormat.parse(currentDate.toString).getTime()
    //println (s"The density at ${formattedDate} is : "+ density2)
    val text= time+","+formattedDate + ","+ totalVertices + ","+ totalEdges + ","+ density2
    writing.writeLines(output_file,text)
    println(println("End: "+ text))

  }

  override protected def processOtherMessages(value: Any): Unit = ""


}
