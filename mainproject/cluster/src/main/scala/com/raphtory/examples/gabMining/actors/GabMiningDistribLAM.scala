package com.raphtory.examples.gabMining.actors

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Date

import com.raphtory.core.components.AnalysisManager.LiveAnalysisManager
import com.raphtory.core.analysis.Analyser
import com.raphtory.core.utils.Utils
import com.raphtory.examples.gabMining.analysis.GabMiningDistribAnalyserIn

import scala.collection.MapLike
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParTrieMap


class GabMiningDistribLAM (jobID:String) extends LiveAnalysisManager (jobID)  {
  //Wed Aug 10 04:59:06 BST 2016
  val inputFormat = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
  val outputFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")

  override protected def defineMaxSteps(): Int = 1

  override protected def generateAnalyzer: Analyser = new GabMiningDistribAnalyserIn()

  override protected def processResults(): Unit = {
    var finalResults=ArrayBuffer[(Int, Int)]()

    for(kv <- results){
      //println("KV RESULTS: "+ kv )
      for(pair <- kv.asInstanceOf[List[(Int, Int)]]){
        finalResults+= pair
      }

    }

    val currentDate=LocalDateTime.now()
    //val formattedDate = outputFormat.format(inputFormat.parse(currentDate.toString))
    var degrees=finalResults.groupBy(_._1).mapValues(seq => seq.map(_._2).reduce(_ + _)).toList.sortBy(_._1)//.foreach(println)
    for ((degree,total)<- degrees){
      var text=currentDate+","+degree+","+total
      Utils.writeLines("results/distribLAM.csv",text)

    }

  }

  override protected def processOtherMessages(value: Any): Unit = ""
}
//INSIDE FINAL Results: ArrayBuffer((0,122), (1,48), (2,1), (3,1), (4,1), (0,119), (5,1), (1,37), (2,9), (3,3), (4,1), (2,7), (1,36), (3,1), (0,135), (0,123), (5,1), (1,42), (2,10), (7,1), (3,2), (0,121), (1,43), (2,9), (3,2), (4,1), (2,8), (1,44), (3,7), (0,125), (0,113), (5,1), (1,42), (2,8), (3,1), (8,1), (2,3), (1,49), (0,119), (0,132), (1,39), (6,1), (2,6), (4,1), (0,183), (1,54), (6,1), (2,9), (3,1))