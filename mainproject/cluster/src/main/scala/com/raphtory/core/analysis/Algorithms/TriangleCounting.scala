package com.raphtory.core.analysis.Algorithms

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.utils.Utils
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.{ParTrieMap}

class TriangleCounting(args:Array[String]) extends Analyser(args){

  override def setup(): Unit =
    println("setting up stuff")
//    proxy.getVerticesSet().foreach { v =>
//      println("sending out messages")
//      val vertex = proxy.getVertex(v._2)
//      val nbsv = vertex.getOutgoingNeighbors.keySet
//      val toSend = vertex.getOrSetCompValue("1hneigh", nbsv).asInstanceOf[List[Long]]
//      vertex.messageAllNeighbours(toSend)
//    }

  override def analyse(): Unit =
    println("supposedly analysing --")

  override def returnResults(): Any =
    println("in results")
//    proxy.getVerticesWithMessages().foreach { vert =>
//    println("going over messages")
//      val node = vert._1
//      val vertex = proxy.getVertex(vert._2)
//      val queue = vertex.messageQueue.map(_.asInstanceOf[List[Long]])
//      if (queue.nonEmpty) {
//        val nbs = vertex.getCompValue("1hneigh").asInstanceOf[List[Long]].toSet
//        val count = queue.map { u =>
//          val comn = (nbs & u.toSet).size
//          (u, comn)
//        }
//        val c = count.toMap.values.sum / 2
//        vertex.clearQueue
//        (node, c)
//      }
//    }
//    proxy
//      .getVerticesSet()
//      .map(vert => (vert._1,vert._2))
//    val v = proxy.getVerticesSet()
//    v.map{vert =>
//      val vertex    = proxy.getVertex(vert._2)
//      (vert._1, vertex.getOutgoingNeighbors.keySet.toList)
//    }

  override def defineMaxSteps(): Int = 10

  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
    val endResuts = results.asInstanceOf[ArrayBuffer[(Long, Int)]]//.flatten.toMap
    val startTime   = System.currentTimeMillis()
    val output_file = System.getenv().getOrDefault("PROJECT_OUTPUT", "/app/output.csv").trim
//    try {
//      endResuts.foreach{x =>
////        val nbs = x._2.toSet
////        val node = x._1
////        val count = nbs.map { u =>
////          val comn = (nbs & endResuts(u.asInstanceOf[Long]).toSet).size
////          (u, comn)
////        }
////        val c = count.toMap.values.sum / 2
////        val text =
////          s"""{"time":$timeStamp,"node":,$node,"triangles":$c,"viewTime":$viewCompleteTime,"concatTime":${
////            System.currentTimeMillis() - startTime}},"""
////        Utils.writeLines(output_file, text, "{\"views\":[")
////        println(text)
//        // publishData(text)
//      }
//    }catch {
//      case e: UnsupportedOperationException => println(s"No activity for  view at $timeStamp")
//    }
  }
//  override def processWindowResults(results: ArrayBuffer[Any],timestamp: Long, windowSize: Long,viewCompleteTime: Long): Unit = {
//    val endResuts = results.asInstanceOf[ArrayBuffer[ParTrieMap[Long,List[Any]]]].flatten.toMap
//    var output_file = System.getenv().getOrDefault("GAB_PROJECT_OUTPUT", "/opt/docker/output.csv").trim
//    val startTime   = System.currentTimeMillis()
//    try{
//      endResuts.foreach{x =>
//        val nbs = x._2.toSet
//        val node = x._1
//        val count = nbs.map { u =>
//          val comn = (nbs & endResuts(u.asInstanceOf[Long]).toSet).size
//          (u, comn)
//        }
//        val c = count.toMap.values.sum / 2
//        val text =
//          s"""{"time":$timestamp,"windowsize":$windowSize,"node":,$node,"triangles":$c,"viewTime":$viewCompleteTime,"concatTime":${
//            System.currentTimeMillis() - startTime}},"""
//        Utils.writeLines(output_file, text, "{\"views\":[")
//        println(text)
//        // publishData(text)
//      }
//    }catch {
//      case e: UnsupportedOperationException => println(s"No activity for  view at $timestamp")
//    }
//  }
//  override def processBatchWindowResults(
//                                          results: ArrayBuffer[Any],
//                                          timestamp: Long,
//                                          windowSet: Array[Long],
//                                          viewCompleteTime: Long
//                                        ): Unit =
//    for (i <- results.indices) {
//      val window     = results(i).asInstanceOf[ArrayBuffer[Any]]
//      val windowSize = windowSet(i)
//      processWindowResults(window, timestamp, windowSize, viewCompleteTime)
//    }

}
