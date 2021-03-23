package com.raphtory.algorithms

import com.raphtory.core.analysis.api.Analyser

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParArray

class TemporalTriangleCount(args:Array[String]) extends Analyser(args) {

  override def setup(): Unit =
    view.getVertices().foreach { vertex =>
      val edgeTimes = vertex.getIncEdges.map(edge => edge.latestActivity()._1)
      val t_max = if(edgeTimes.nonEmpty) edgeTimes.max else -1 //get incoming edgeTimes and then find the most recent edge with respect to timestamp and window
      vertex.getOutEdgesBefore(t_max).foreach(neighbour => {
        neighbour.send((Array(vertex.ID()),t_max))
      })
    }

  override def analyse(): Unit =
    view.getMessagedVertices().foreach { vertex =>
      val queue = vertex.messageQueue[(Array[Long], Long)]
      queue.foreach(message=> {
        val path = message._1
        val sender = path(path.length-1)
        val t_max = message._2
        val t_min = vertex.getInEdge(sender).get.earliestActivity()._1 //to include deletions check
        if(path.length<2) { //for step two of the algorithm i.e. the second node in the triangle
          vertex.getOutEdgesBetween(t_min, t_max).foreach(neighbour => {
            neighbour.send((message._1 ++ Array(vertex.ID()), t_max))
          })
        }
        else{ //for the 3rd node in the triangle to see if the final edge exists
          val source = path(0)
          vertex.getOutEdgeBetween(source,t_min,t_max) match {
            case Some(edge) =>
              vertex.appendToState("TrianglePath",(path ++ Array(vertex.ID())).mkString("["," ","]"))
            case None => //No triangle for you
          }
        }
      })
    }

  override def returnResults(): Any =
    view.getVertices()
      .filter(vertex=>
        vertex.containsState("TrianglePath"))
      .flatMap(vertex =>
        vertex.getState[Array[String]]("TrianglePath")).to



  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[ParArray[String]]].flatten.toArray
    val toPublish = s"""{"timestamp":$timeStamp,triangles:[""" +endResults.map(triangle => triangle+",").fold("")(_+_).dropRight(1)+"]}"
    println(toPublish)
    publishData(toPublish)
  }

  override def defineMaxSteps(): Int = 2
}