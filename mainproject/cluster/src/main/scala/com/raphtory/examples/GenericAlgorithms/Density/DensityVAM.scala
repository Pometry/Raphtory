
package com.raphtory.examples.GenericAlgorithms.Density

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Date

import com.raphtory.core.analysis.Analyser
import com.raphtory.core.components.AnalysisManager.ViewAnalysisManager
import com.raphtory.core.utils.Utils

import scala.collection.mutable.ArrayBuffer


class DensityVAM(jobID:String, start:Long)extends ViewAnalysisManager (jobID,start){

  val output_file = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/app/defout.csv").trim
  //Wed Aug 10 04:59:06 BST 2016
  val inputFormat = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
  val outputFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
  Utils.writeLines(output_file,"Time,Date,TotalVertices,TotalEdges,Density")

  override protected def defineMaxSteps(): Int = 1

  override protected def generateAnalyzer: Analyser = new DensityAnalyser()

  override protected def processResults(): Unit = {

    var totalVertices=0
    var totalEdges=0

    var allResults=results.asInstanceOf[ArrayBuffer[(Int,Int)]]
    //  println("WHAT CAME FROM ANALYSER: "+allResults)
    for (verticesAndEdges <- allResults){
      totalVertices+=verticesAndEdges._1
      totalEdges+=verticesAndEdges._2

    }
    //println(f"T`otal vertices: "+ totalVertices + " Total edges: "+ totalEdges + " Density: "+density)
    val density : Float= (totalEdges.toFloat/(totalVertices.toFloat*(totalVertices.toFloat-1)))
    //println(f"Total vertices: "+ totalVertices + " Total edges: "+ totalEdges + " Density: "+density)
    val currentDate=new Date(timestamp())
    val formattedDate = outputFormat.format(inputFormat.parse(currentDate.toString))
    var time = inputFormat.parse(currentDate.toString).getTime()
    val density2=new java.math.BigDecimal(density).toPlainString
    println (s"The density at ${formattedDate} is : "+ density2)
    val text= time+","+formattedDate + ","+ totalVertices + ","+ totalEdges + ","+density2
    Utils.writeLines(output_file,text)
    println(println("End: "+ LocalDateTime.now()))

  }

  override protected def processOtherMessages(value: Any): Unit = ""


}

