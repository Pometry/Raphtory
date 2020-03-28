package com.raphtory.examples.random.depricated

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Date

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.utils.Utils

import scala.collection.mutable.ArrayBuffer

// to obtain the density of the network we need to obtain the degree and the number of vertices to plug in the
// formula that is executed by the Live Analysis.
//For this , we need to loop over the vertices that are currently in memory in the system, then for each vertex ,
//we need to get the number of its neighbours so we can obtain the number of edges to be sent.
//the values are sent in a simple array tuple formed by two integers

//Initialisation of the file in where the output will be written is done.
//The partial results sent from the GabMiningDensityAnalyser are read and store in a data structure similar
//to the one used by the analyser. This is, an array of tuples of two Int values.
//for each of the values sent by the analyser, we sum their values so we can get the final summarisation
//for the total of the values of how many vertices and edges are in the system.
// Then we plug these values into the final formula to output the density to file that is written.

class Density extends Analyser {
  var output_file  = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/app/defout.csv").trim
  val inputFormat  = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy")
  val outputFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")

  override def analyse(): Unit = {
    var totalDegree: Int = 0
    var totalNodes: Int  = 0
    for (v <- proxy.getVerticesSet()) {
      val vertex = proxy.getVertex(v._2)
      val degree = vertex.getIngoingNeighbors.size

      totalDegree += degree
      totalNodes += 1

    }
    (totalNodes, totalDegree)

  }
  override def setup(): Unit = {}

  override def defineMaxSteps(): Int = 1
  var totalVertices                  = 0
  var totalEdges                     = 0
  var density2                       = "0"
  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
    for (verticesAndEdges <- results.asInstanceOf[ArrayBuffer[(Int, Int)]]) {
      totalVertices += verticesAndEdges._1
      totalEdges += verticesAndEdges._2
    }
    if (totalVertices >= 2) {
      val density: Float = (totalEdges.toFloat / (totalVertices.toFloat * (totalVertices.toFloat - 1)))
      density2 = new java.math.BigDecimal(density).toPlainString
    }
    val currentDate = LocalDateTime.now()
    println(s"The density at $currentDate is : " + density2)
  }
  override def processViewResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
    for (verticesAndEdges <- results.asInstanceOf[ArrayBuffer[(Int, Int)]]) {
      totalVertices += verticesAndEdges._1
      totalEdges += verticesAndEdges._2
    }
    if (totalVertices >= 2) {
      val density: Float = (totalEdges.toFloat / (totalVertices.toFloat * (totalVertices.toFloat - 1)))
      density2 = new java.math.BigDecimal(density).toPlainString
    }
    val currentDate = new Date(timestamp)
    Utils.writeLines(
            output_file,
            inputFormat.parse(currentDate.toString).getTime() + "," + outputFormat.format(
                    inputFormat.parse(currentDate.toString)
            ) + "," + totalVertices + "," + totalEdges + "," + density2,
            "Time,Date,TotalVertices,TotalEdges,Density"
    )
    println(println("End: " + LocalDateTime.now()))
  }

  override def processWindowResults(
      results: ArrayBuffer[Any],
      timestamp: Long,
      windowSize: Long,
      viewCompleteTime: Long
  ): Unit = processResults(results, timestamp, viewCompleteTime)

  override def processBatchWindowResults(
      results: ArrayBuffer[Any],
      timestamp: Long,
      windowSet: Array[Long],
      viewCompleteTime: Long
  ): Unit = {
    val endResults                                       = results.asInstanceOf[ArrayBuffer[ArrayBuffer[(Int, Int)]]]
    val pariFold: ((Int, Int), (Int, Int)) => (Int, Int) = (a, b) => (a._1 + b._1, a._2 + b._2)
    for (i <- endResults.indices) {
      val totals = endResults(i).fold(0, 0)(pariFold)
      totalVertices = totals._1
      totalEdges = totals._2
      println(s"$timestamp $totalEdges $totalVertices")
      if (totalVertices >= 2) {
        val density: Float = (totalEdges.toFloat / (totalVertices.toFloat * (totalVertices.toFloat - 1)))
        density2 = new java.math.BigDecimal(density).toPlainString
      }
      val currentDate = new Date(timestamp)
      val text        = s"""{"time":$timestamp,"windowsize":${windowSet(i)},"density":$density2},"""
      Utils.writeLines(output_file, text, "{\"views:[\"")
      density2 = "0"

    }
    //println(println("End: "+ LocalDateTime.now()))
  }

  override def returnResults(): Any = ???
}
