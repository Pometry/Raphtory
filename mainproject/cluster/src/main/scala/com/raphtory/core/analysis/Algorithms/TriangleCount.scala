package com.raphtory.core.analysis.Algorithms

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.utils.Utils

import scala.collection.mutable.ArrayBuffer

/** Each vertex wants to check how many pairs of its neighbours are connected. It does this by messaging all its
  * neighbours the list of neighbours with id greater than its own. So if I'm vertex 3 and my neighbours are 2, 5 and 7
  * I will send the message '5' and '7' to my neighbours. Then each vertex checks its incoming messages and sees if any
  * of the messages received correspond to ids of its neighbours. */

class TriangleCount(args:Array[String]) extends Analyser(args) {

  override def setup(): Unit = {
    view.getVertices().foreach { vertex =>
      vertex.setState("triangles", 0)
      val neighbours = vertex.getIncEdges.map(x=> x.ID()).toSet.union(vertex.getOutEdges.map(x=> x.ID()).toSet).filter(x=> x>vertex.ID())
      vertex.messageAllNeighbours(neighbours)
    }
  }

  override def analyse(): Unit = {
      view.getMessagedVertices().foreach { vertex =>
        val neighbours = vertex.getIncEdges.map(x=> x.ID()).toSet.union(vertex.getOutEdges.map(x=> x.ID()).toSet)
        val queue = vertex.messageQueue[Set[Long]].flatten
        val totalTriangles = queue.toSet.intersect(neighbours).size
        vertex.setState("triangles",totalTriangles)
      }

  }

  override def returnResults(): Any = {
    val triangleStats  = view.getVertices().map {
      vertex => (vertex.getState[Int]("triangles"), vertex.getOutEdges.size + vertex.getIncEdges.size)
    }.map {
      row => (row._1, if (row._1 > 1) 2.0*row._1/(row._2*(row._2 - 1)) else 0.0 )
    }
    val totalV = triangleStats.size
    val totalTri = triangleStats.map(x => x._1).sum
    val totalCluster = triangleStats.map(x => x._2).sum
    (totalV, totalTri, totalCluster)
  }

  override def defineMaxSteps(): Int = 5

  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
    val startTime   = System.currentTimeMillis()
    val endResults = results.asInstanceOf[ArrayBuffer[(Int, Int, Double)]]
    val totalVert = endResults.map( x => x._1 ).sum
    val totalTri = endResults.map( x => x._2 ).sum/3
    val avgCluster = if (totalVert > 0) endResults.map( x => x._3 ).sum/totalVert else 0.0
//    val clusterCoeff =
//      try endResults.map(x => x._3).sum/totalVert.toFloat
//      catch { case e: ArithmeticException => 0.0 }
    val text = s"""{"time":$timeStamp,"totTriangles":$totalTri,"avgCluster":$avgCluster,"viewTime":$viewCompleteTime,"concatTime":${System
      .currentTimeMillis() - startTime}},"""
    println(text)
  }

  override def processWindowResults(results: ArrayBuffer[Any], timestamp: Long, windowSize: Long, viewCompleteTime: Long): Unit = {
    val startTime   = System.currentTimeMillis()
    val endResults = results.asInstanceOf[ArrayBuffer[(Int, Int, Double)]]
    var output_file = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/app/defout.csv").trim

    val totalVert = endResults.map( x => x._1 ).sum
    val totalTri = endResults.map( x => x._2 ).sum/3
    val avgCluster = if (totalVert > 0) endResults.map( x => x._3 ).sum/totalVert else 0.0
    //    val clusterCoeff =
    //      try endResults.map(x => x._3).sum/totalVert.toFloat
    //      catch { case e: ArithmeticException => 0.0 }
    val text =
      s"""{"time":$timestamp,"windowsize":$windowSize,"totTriangles":$totalTri,"avgCluster":$avgCluster,"viewTime":$viewCompleteTime,"concatTime":${System
        .currentTimeMillis() - startTime}},"""
    Utils.writeLines(output_file, text, "{\"views\":[")
    println(text)
  }
}
