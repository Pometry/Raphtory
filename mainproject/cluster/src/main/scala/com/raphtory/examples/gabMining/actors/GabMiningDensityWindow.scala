

package com.raphtory.examples.gabMining.actors

import java.util.Date

import com.raphtory.core.analysis.Analyser
import com.raphtory.core.components.AnalysisManager.{RangeAnalysisManager, WindowRangeAnalysisManager}
import com.raphtory.examples.gabMining.analysis.GabMiningDensityAnalyser
import com.raphtory.examples.gabMining.utils.writeToFile

import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat
import java.time.LocalDateTime

//Initialisation of the file in where the output will be written is done.
//The partial results sent from the GabMiningDensityAnalyser are read and store in a data structure similar
//to the one used by the analyser. This is, an array of tuples of two Int values.
//for each of the values sent by the analyser, we sum their values so we can get the final summarisation
//for the total of the values of how many vertices and edges are in the system.
// Then we plug these values into the final formula to output the density to file that is written.
class GabMiningDensityWindow (jobID:String,start:Long,end:Long,jump:Long,window:Long)extends WindowRangeAnalysisManager (jobID,start,end,jump,window){
  val output_file = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/app/defout.csv").trim
  val writing=new writeToFile()
  //Wed Aug 10 04:59:06 BST 2016
  val inputFormat = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
  val outputFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
  writing.writeLines(output_file,"Time,Date,TotalVertices,TotalEdges,Density")

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
    println(println("End: "+ LocalDateTime.now()))


  }

  override protected def processOtherMessages(value: Any): Unit = ""

}

