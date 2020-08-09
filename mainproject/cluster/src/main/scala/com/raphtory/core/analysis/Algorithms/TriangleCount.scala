package com.raphtory.core.analysis.Algorithms

import com.raphtory.core.analysis.API.Analyser

import scala.collection.mutable.ArrayBuffer

/** Each vertex wants to check how many pairs of its neighbours are connected. It does this by messaging all its
  * neighbours the list of neighbours with id greater than its own. So if I'm vertex 3 and my neighbours are 2, 5 and 7
  * I will send the message '5' and '7' to my neighbours. Then each vertex checks its incoming messages and sees if any
  * of the messages received correspond to ids of its neighbours. */

class TriangleCount(args:Array[String]) extends Analyser(args) {

  override def setup(): Unit =
    view.getVertices().foreach { vertex =>
      vertex.setState("triangles",0)
      vertex.setState("cluster",0.0)
      val degree = vertex.getIncEdges.size + vertex.getOutEdges.size
      vertex.messageAllNeighbours(vertex.ID())
    }

  override def analyse(): Unit = {
    view.getMessagedVertices().foreach { vertex =>
      val neighbours = vertex.messageQueue[Long]
      vertex.setState("neighbours", neighbours)
      neighbours.foreach { neighbour =>
        // Avoid repeated triangles
        if (neighbour > vertex.ID()) {
          vertex.messageAllNeighbours(neighbour)
        }
      }
    }
    view.getMessagedVertices().foreach { vertex =>
      val queue = vertex.messageQueue[Long]
      val neighbours = vertex.getState("neighbours")
      queue.foreach { message =>
        if (neighbours.asInstanceOf[Array[Long]].contains(message)) {
          val newlabel = vertex.getState[Int]("triangles") + 1
          vertex.setState("triangles", newlabel)
        }
      }
      vertex.voteToHalt()
    }
  }

  override def returnResults(): Any = {
    val triangleStats  = view.getVertices().map {
      vertex => (vertex.getState[Int]("triangles"))
    }.sum
//    val triangleStats = view.getVertices().map { vertex =>
//      val degree = vertex.getOutEdges.size + vertex.getIncEdges.size
//      val triangle = vertex.getState[Int]("triangles")
//      val cluster =
//        try vertex.getState[Float]("triangles")/2.0*degree*(degree-1)
//        catch { case e: ArithmeticException => 0.0 }
//      (vertex.ID(), triangle, cluster)
//    }
//    val totalV   = triangleStats.size
//    val totTri = (triangleStats.map(x => x._2).sum/3).toInt
//    val clusterCoeff = triangleStats.map(x => x._3).sum
//    (totalV,totTri,clusterCoeff)
  }

  override def defineMaxSteps(): Int = 1

  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
    val startTime   = System.currentTimeMillis()
    val endResults = results.asInstanceOf[ArrayBuffer[Int]]
    val totalTri = endResults.map(x => x).sum
//    val clusterCoeff =
//      try endResults.map(x => x._3).sum/totalVert.toFloat
//      catch { case e: ArithmeticException => 0.0 }
    val text = s"""{"time":$timeStamp,"totTriangles":$totalTri,"viewTime":$viewCompleteTime,"concatTime":${System
      .currentTimeMillis() - startTime}},"""
  }
}
