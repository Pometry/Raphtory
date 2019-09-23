package com.raphtory.core.analysis.Algorithms

import java.text.SimpleDateFormat
import java.util.Date

import com.raphtory.core.analysis.API.{Analyser, WorkerID}
import com.raphtory.core.utils.Utils

import scala.collection.mutable.ArrayBuffer

class OutDegree extends Analyser{

  override def analyse(): Any = {
    var results = ArrayBuffer[Int]()
    proxy.getVerticesSet().foreach(v => {

      val vertex = proxy.getVertex(v._2)
      val totalEdges= vertex.getOutgoingNeighbors.size
    //  println("Total edges for V "+v+" "+vertex.getOutgoingNeighbors + " "+vertex.getIngoingNeighbors )
      results+=totalEdges
    })
    // println("THIS IS HOW RESULTS LOOK: "+ results.groupBy(identity).mapValues(_.size))
    results.groupBy(identity).mapValues(_.size).toList
  }
  override def setup(): Any = {

  }

  override def defineMaxSteps(): Int = 1

  override def processResults(results: ArrayBuffer[Any], oldResults: ArrayBuffer[Any],viewCompleteTime:Long): Unit = {

  }

  override def processViewResults(results: ArrayBuffer[Any], oldResults: ArrayBuffer[Any], timestamp: Long,viewCompleteTime:Long): Unit = {
    val output_file = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/app/defout.csv").trim

    val inputFormat = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
    val outputFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
    var finalResults = ArrayBuffer[(Int, Int)]()

    for (kv <- results) {
      // println("KV RESULTS: " + kv)
      for (pair <- kv.asInstanceOf[List[(Int, Int)]]) {
        finalResults += pair
      }

    }

    val currentDate = new Date(timestamp)
    val formattedDate = outputFormat.format(inputFormat.parse(currentDate.toString))
    var degrees = finalResults.groupBy(_._1).mapValues(seq => seq.map(_._2).reduce(_ + _)).toList.sortBy(_._1) //.foreach(println)
    for ((degree, total) <- degrees) {
      var text = formattedDate + "," + degree + "," + total
      Utils.writeLines(output_file, text,"Date,OutDegree,Total")

    }
  }

  override def processWindowResults(results: ArrayBuffer[Any], oldResults: ArrayBuffer[Any], timestamp: Long, windowSize: Long,viewCompleteTime:Long): Unit = ???
}

