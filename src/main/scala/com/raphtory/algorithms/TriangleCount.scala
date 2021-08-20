package com.raphtory.algorithms

import com.raphtory.core.analysis.api.Analyser

import scala.collection.mutable.ArrayBuffer

/** Each vertex wants to check how many pairs of its neighbours are connected. It does this by messaging all its
  * neighbours the list of neighbours with id greater than its own. So if I'm vertex 3 and my neighbours are 2, 5 and 7
  * I will send the message '5' and '7' to my neighbours. Then each vertex checks its incoming messages and sees if any
  * of the messages received correspond to ids of its neighbours. */

class TriangleCount(args:Array[String]) extends Analyser[(Int,Int,Double)](args) {

  override def setup(): Unit = {
    view.getVertices().foreach { vertex =>
      vertex.setState("triangles", 0)
      val neighbours = vertex.getIncEdges.map(x=> x.ID).toSet.union(vertex.getOutEdges.map(x=> x.ID).toSet).seq.filter(_ !=vertex.ID())
      val toSend = neighbours.seq.filter(_ > vertex.ID())
      neighbours.foreach { nb =>
        vertex.messageNeighbour(nb, toSend)
      }
    }
  }

  override def analyse(): Unit = {
      view.getMessagedVertices().foreach { vertex =>
        val neighbours = vertex.getIncEdges.map(x=> x.ID()).toSet.union(vertex.getOutEdges.map(x=> x.ID()).toSet).seq.filter(_ != vertex.ID())
        val queue = vertex.messageQueue[Set[Long]]
        var totalTriangles = 0
        queue.foreach { nbs =>
          totalTriangles += nbs.intersect(neighbours).size
        }
        vertex.setState("triangles",totalTriangles)
      }

  }

  override def returnResults(): (Int,Int,Double) = {
    val triangleStats  = view.getVertices().map {
      vertex => (vertex.getState[Int]("triangles"), vertex.getOutEdges.size + vertex.getIncEdges.size)
    }.map {
      row => (row._1, if (row._2 > 1) 2.0*row._1/(row._2*(row._2 - 1)) else 0.0 )
    }
    val totalV = triangleStats.size
    val totalTri = triangleStats.map(x => x._1).sum
    val totalCluster = triangleStats.map(x => x._2).sum
    (totalV, totalTri, totalCluster)
  }

  override def defineMaxSteps(): Int = 5

  override def extractResults(results: List[(Int,Int,Double)]): Map[String, Any] = {
    val startTime   = System.currentTimeMillis()
    val endResults = results
    val totalVert = endResults.map( x => x._1 ).sum
    val totalTri = endResults.map( x => x._2 ).sum/3
    val avgCluster = if (totalVert > 0) endResults.map( x => x._3 ).sum/totalVert else 0.0
//    val clusterCoeff =
//      try endResults.map(x => x._3).sum/totalVert.toFloat
//      catch { case e: ArithmeticException => 0.0 }
//    val text = s"""{"totTriangles":$totalTri,"avgCluster":$avgCluster,"concatTime":${System
//      .currentTimeMillis() - startTime}},"""
//    println(text)
    Map[String,Any]("vertices"->totalVert,"totalTri"->totalTri,"avgCluster"->avgCluster)
  }

}