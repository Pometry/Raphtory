package com.raphtory.examples.random.depricated

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Date

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.utils.Utils

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParTrieMap

//A star is about to find the maximum in degree and the node that has it at a given time . For this we need to
//parse the vertices that currently exist in the system and get the number of their ingoingNeighbours and we
//sent to the analyser only the vertex  that had the maximum in-degree value. We sent it as a key value pair to
// the analyser.
//
class StarNode(args:Array[String]) extends Analyser(args){

  override def analyse(): Unit = {
    val results = ParTrieMap[Int, Int]()

    for (v <- view.getVerticesSet()) {
      val vertex   = view.getVertex(v._2)
      val inDegree = vertex.getIncEdges.size
      //println("CHeck this out:"+v.toInt+" "+ inDegree.toInt)
      results.put(v._1.toInt, inDegree.toInt)

    }
    if (results.nonEmpty) {
      val max = results.maxBy(_._2)
      (max._1, max._2)
    }

  }

  override def setup(): Unit = {}

  override def defineMaxSteps(): Int = 1

  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
    val inputFormat  = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    val outputFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
    //val finalResults=ParTrieMap[Int, Int]()
    val currentDate   = LocalDateTime.now()
    val formattedDate = outputFormat.format(inputFormat.parse(currentDate.toString))
    // var time = inputFormat.parse(currentDate.toString).getTime()

    for (result <- results)
      if (result != ()) { //println("No null "+ v +" "+ v.getClass)
        val stars = result.asInstanceOf[(Int, Int)]
        println(s"The star at $formattedDate is : ${stars._1} with ${stars._2} in degree")
        val text = s"$formattedDate,${stars._1}, ${stars._2}"
        Utils.writeLines("results/Stars/starsLive.csv", text, "Date,Vertex,InDegree")
        //finalResults.put(stars._1,stars._2)
      }
  }
  //Initialisation of the file in where the output will be written is done.
  // the stars identified in the GabMiningStarsAnalyser are received and then , analysed one more time to obtain
  //only the real star from all the ones that were sent.
  //So from the list sent, we perform another maxBy_._ to get the higest in-degree value (which came in the position 2 of
  //the tuple)
  //the final value is written to the file.
  override def processViewResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
    val inputFormat   = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
    val outputFormat  = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
    val finalResults  = ParTrieMap[Int, Int]()
    val currentDate   = new Date(timestamp)
    val formattedDate = outputFormat.format(inputFormat.parse(currentDate.toString))
    // var time = inputFormat.parse(currentDate.toString).getTime()

    for (result <- results)
      if (result != ()) { //println("No null "+ v +" "+ v.getClass)
        val stars = result.asInstanceOf[(Int, Int)]
        // println (s"The star at ${formattedDate} is : ${stars._1} with ${stars._2} in degree")
        finalResults.put(stars._1, stars._2)
      }

    var printfinalResults = finalResults.maxBy(_._2)
    val text              = s"$formattedDate,${printfinalResults._1},${printfinalResults._2}"

    Utils.writeLines("results/Stars/starsRange.csv", text, "Date,Vertex,InDegree")
  }

  //Initialisation of the file in where the output will be written is done.
  // the stars identified in the GabMiningStarsAnalyser are received and then , analysed one more time to obtain
  //only the real star from all the ones that were sent.
  //So from the list sent, we perform another maxBy_._ to get the higest in-degree value (which came in the position 2 of
  //the tuple)
  //the final value is written to the file.

  override def processWindowResults(
      results: ArrayBuffer[Any],
      timestamp: Long,
      windowSize: Long,
      viewCompleteTime: Long
  ): Unit = {
    val output_file   = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/app/defout.csv").trim
    val inputFormat   = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
    val outputFormat  = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
    val finalResults  = ParTrieMap[Int, Int]()
    val currentDate   = new Date(timestamp)
    val formattedDate = outputFormat.format(inputFormat.parse(currentDate.toString))
    // var time = inputFormat.parse(currentDate.toString).getTime()

    for (result <- results)
      if (result != ()) { //println("No null "+ v +" "+ v.getClass)
        val stars = result.asInstanceOf[(Int, Int)]
        // println (s"The star at ${formattedDate} is : ${stars._1} with ${stars._2} in degree")
        finalResults.put(stars._1, stars._2)
      }

    var printfinalResults = finalResults.maxBy(_._2)
    val text              = s"$formattedDate,${printfinalResults._1},${printfinalResults._2}"
    Utils.writeLines(output_file, text, "Date,Vertex,InDegree")
    // println (s"The star at ${new Date(timestamp())} is : ${finalResults.maxBy(_._2)}")

  }

  override def returnResults(): Any = ???
}
