package com.raphtory.examples.gabMining.actors

import java.text.SimpleDateFormat
import java.util.Date

import com.raphtory.core.analysis.Analyser
import com.raphtory.core.components.AnalysisManager.RangeAnalysisManager
import com.raphtory.examples.gabMining.analysis.GabMiningStarsAnalyser
import com.raphtory.examples.gabMining.utils.writeToFile

import scala.collection.parallel.mutable.ParTrieMap


//Initialisation of the file in where the output will be written is done.
// the stars identified in the GabMiningStarsAnalyser are received and then , analysed one more time to obtain
//only the real star from all the ones that were sent.
//So from the list sent, we perform another maxBy_._ to get the higest in-degree value (which came in the position 2 of
//the tuple)
//the final value is written to the file.

class GabMiningStarsRange(jobID:String, start:Long, end:Long, jump:Long)extends RangeAnalysisManager (jobID,start,end,jump){
  val writing=new writeToFile()
  val output_file = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/app/defout.csv").trim

  val inputFormat = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
  val outputFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
  writing.writeLines(output_file,"Date,Vertex,InDegree")



  override protected def defineMaxSteps(): Int = 1

  override protected def generateAnalyzer: Analyser = new GabMiningStarsAnalyser()

  override protected def processResults(): Unit = {

    val finalResults=ParTrieMap[Int, Int]()
    val currentDate=new Date(timestamp())
    val formattedDate = outputFormat.format(inputFormat.parse(currentDate.toString))
    // var time = inputFormat.parse(currentDate.toString).getTime()

    for (result <- results) {
      if(result!=()){ //println("No null "+ v +" "+ v.getClass)
        val stars=result.asInstanceOf[(Int,Int)]
        // println (s"The star at ${formattedDate} is : ${stars._1} with ${stars._2} in degree")
        finalResults.put(stars._1,stars._2)
      }
    }

    var printfinalResults=finalResults.maxBy(_._2)
    val text= s"${formattedDate},${printfinalResults._1},${printfinalResults._2}"
    writing.writeLines(output_file,text)
    // println (s"The star at ${new Date(timestamp())} is : ${finalResults.maxBy(_._2)}")

  }

  override protected def processOtherMessages(value: Any): Unit = ""

}
