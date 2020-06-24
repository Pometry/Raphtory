package com.raphtory.core.analysis.Algorithms

import com.raphtory.core.analysis.API.Analyser

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.{ParSet, ParTrieMap}

class TriangleCounting(args:Array[String]) extends Analyser(args){
  override def analyse(): Unit = {}

  override def setup(): Unit = {}

  override def returnResults(): Any = {
    val v = proxy.getVerticesSet()
    v.map{vert =>
      val vertex    = proxy.getVertex(vert._2)
      (vert._1, vertex.getOutgoingNeighbors.keySet)
    }
  }

  override def defineMaxSteps(): Int = 1

  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
    val endResuts = results.asInstanceOf[ArrayBuffer[ParTrieMap[Long,Set[Any]]]].flatten.toMap
    endResuts.foreach{x=>
    val nbs = x._2//.toSet //- Set(x._1)
      val node=x._1
      //println("treating node ", node)
    val count = nbs.map{u=>
      (nbs & endResuts(u.asInstanceOf[Long])).size
      }

      val c = count.sum
      println(s"""$node, $c""")
    }
  }

}
