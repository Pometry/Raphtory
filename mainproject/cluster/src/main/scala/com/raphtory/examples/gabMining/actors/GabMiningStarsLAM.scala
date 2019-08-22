package com.raphtory.examples.gabMining.actors

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Date

import com.raphtory.core.components.AnalysisManager.LiveAnalysisManager
import com.raphtory.core.analysis.Analyser
import com.raphtory.core.utils.Utils
import com.raphtory.examples.gabMining.analysis.GabMiningStarsAnalyser

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParTrieMap

class GabMiningStarsLAM(jobID:String)extends LiveAnalysisManager (jobID)  {

  val inputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
  val outputFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
  Utils.writeLines("results/Stars/starsRange.csv","Date,Vertex,InDegree")


  override protected def defineMaxSteps(): Int = 1

  override protected def generateAnalyzer: Analyser = new GabMiningStarsAnalyser()

  override protected def processResults(): Unit = {

    //val finalResults=ParTrieMap[Int, Int]()
    val currentDate= LocalDateTime.now()
    val formattedDate = outputFormat.format(inputFormat.parse(currentDate.toString))
    // var time = inputFormat.parse(currentDate.toString).getTime()

    for (result <- results) {
      if(result!=()){ //println("No null "+ v +" "+ v.getClass)
        val stars=result.asInstanceOf[(Int,Int)]
        println (s"The star at ${formattedDate} is : ${stars._1} with ${stars._2} in degree")
        val text= s"${formattedDate},${stars._1}, ${stars._2}"
        Utils.writeLines("results/Stars/starsLive.csv",text)
        //finalResults.put(stars._1,stars._2)
      }
    }

    //  println("FINAL TEN: "+finalResults)
    // println (s"The star at ${new Date(timestamp())} is : ${finalResults.maxBy(_._2)}")

  }
  override protected def processOtherMessages(value: Any): Unit = ""

}
//
//var maxInDegree=0
//var maxVertex=0
//// println("LAM RECEIVED RESULTS: "+ results)
//
//
//for ((vertex, degree) <- results){
//// println("********SUUBBBB LAAAMMM Vertex: "+ vertex +"Edges : "+ degree)
//if(degree.toString.toInt>maxInDegree) {
//maxInDegree= degree.toString.toInt
//maxVertex=vertex.toString.toInt
//
//}
//
//}
//println(" Vertex : " + maxVertex + " is a star with " + maxInDegree + " incoming edges")
//var text = maxVertex + "," + maxInDegree
//writing.writeLines("stars.csv", text)