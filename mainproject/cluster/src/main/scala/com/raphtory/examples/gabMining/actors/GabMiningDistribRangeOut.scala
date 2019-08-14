package com.raphtory.examples.gabMining.actors

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Date

import com.raphtory.core.analysis.Analyser
import com.raphtory.core.components.AnalysisManager.RangeAnalysisManager
import com.raphtory.examples.gabMining.analysis.GabMiningDistribAnalyserOut
import com.raphtory.examples.gabMining.utils.writeToFile

import scala.collection.mutable.ArrayBuffer

class GabMiningDistribRangeOut(jobID:String, start:Long, end:Long, jump:Long)extends RangeAnalysisManager (jobID,start,end,jump){
  val output_file = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/app/defout.csv").trim
  val writing=new writeToFile()
  writing.writeLines(output_file,"Date,OutDegree,Total")

  //Wed Aug 10 04:59:06 BST 2016
  val inputFormat = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
  val outputFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")

  override protected def defineMaxSteps(): Int = 1

  override protected def generateAnalyzer: Analyser = new GabMiningDistribAnalyserOut()

  override protected def processResults(): Unit = {
    var finalResults = ArrayBuffer[(Int, Int)]()

    for (kv <- results) {
      // println("KV RESULTS: " + kv)
      for (pair <- kv.asInstanceOf[List[(Int, Int)]]) {
        finalResults += pair
      }

    }

    val currentDate = new Date(timestamp())
    val formattedDate = outputFormat.format(inputFormat.parse(currentDate.toString))
    var degrees = finalResults.groupBy(_._1).mapValues(seq => seq.map(_._2).reduce(_ + _)).toList.sortBy(_._1) //.foreach(println)
    for ((degree, total) <- degrees) {
      var text = formattedDate + "," + degree + "," + total
      writing.writeLines(output_file, text)

    }
  }


  override protected def processOtherMessages(value: Any): Unit = ""

}
