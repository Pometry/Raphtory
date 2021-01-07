package com.raphtory.algorithms

import com.raphtory.api.Analyser
import com.raphtory.core.model.analysis.entityVisitors.VertexVisitor

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable
import scala.collection.parallel.mutable.ParArray

class LPA(args:Array[String]) extends Analyser(args){ //TODO needs Major cleanup
  val arg = args.map(_.trim)//.head
  val top_c = if (arg.length==0 | arg.head=="-1") 0 else arg.head.toInt
  val PROP = "weight"
  val output_file = System.getenv().getOrDefault("OUTPUT_PATH", "/app/out.json").trim

  override def setup(): Unit = {
    view.getVertices().foreach { vertex =>
      val lab = scala.util.Random.nextLong()
      vertex.setState("lpalabel", lab)
      vertex.messageAllNeighbours((vertex.ID(),lab))
    }
  }

  override def analyse(): Unit = {
    view.getMessagedVertices().foreach { vertex =>
    try {
      val vlabel = vertex.getState[Long]("lpalabel")//, scala.util.Random.nextLong())
      val vneigh = vertex.getOutEdges ++ vertex.getIncEdges
      val neigh_freq = vneigh.map { e => (e.ID(), e.getPropertyValue(PROP).getOrElse(1L).asInstanceOf[Long]) }
        .groupBy(_._1).mapValues(x=> x.map(_._2).sum)
      val vfreq = if (vneigh.nonEmpty) neigh_freq.values.sum / vneigh.map(_.ID()).toSet.size else 1L
      val gp = vertex.messageQueue[(Long, Long)].map { v => (v._2, if (neigh_freq.contains(v._1)) neigh_freq(v._1) else 1) }
      gp.append((vlabel, vfreq))
      val newLabel = gp.groupBy(_._1).mapValues(_.map(_._2).sum).maxBy(_._2)._1
      if (newLabel == vlabel) {
        vertex.voteToHalt()
      } else {
        vertex.setState("lpalabel", newLabel)
      }
      vertex.messageAllNeighbours((vertex.ID(), newLabel))
      doSomething(vertex, gp.dropRight(1).map(_._1).toArray)
    }catch{
      case e: Exception => println(e, vertex.ID())
    }
    }
  }

  def labelProbability(gp: Map[Long, ArrayBuffer[Long]]): Long ={
//    label probability function to get rid of oscillation phenomena of synchronous LPA from [1]
//    [1] https://doi.org/10.1016/j.neucom.2014.04.084

    val f = gp.map{lab =>  Math.pow(lab._2.sum, 2)}//TODO: implement full method from paper
    val p = f.toArray.map(i => i/f.toArray.sum)
    val gpp = gp.keys zip p
    gpp.filter(x=>x._2 == gpp.maxBy(_._2)._2).maxBy(_._1)._1
  }

  def doSomething(v: VertexVisitor, gp: Array[Long]): Unit = {}

  override def returnResults(): Any =
    view.getVertices()
      .map(vertex => (vertex.getState[Long]("lpalabel"), vertex.ID()))
      .groupBy(f => f._1)
      .map(f => (f._1, f._2.map(_._2)))


  override def processResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
    val er = extractData(results)
    val commtxt = er.communities.map{x=> s"""["${x.mkString("\",\"")}"]"""}
    val text = s"""{"time":$timestamp,"top5":[${er.top5.mkString(",")}],"total":${er.total},"totalIslands":${er.totalIslands},"proportion":${er.proportion}, "communities":[${commtxt.mkString(",")}],"viewTime":$viewCompleteTime}"""
   // writeLines(output_file, text, "{\"views\":[")
    println(text)
    publishData(text)
  }

  override def processWindowResults(results: ArrayBuffer[Any], timestamp: Long, windowSize: Long, viewCompleteTime: Long): Unit = {
    val er = extractData(results)
    val commtxt = er.communities.map{x=> s"""[${x.mkString(",")}]"""}
    val text = s"""{"time":$timestamp,"windowsize":$windowSize,"top5":[${er.top5.mkString(",")}],"total":${er.total},"totalIslands":${er.totalIslands},"communities": [${commtxt.mkString(",")}],"proportion":${er.proportion}, "viewTime":$viewCompleteTime},"""
   // writeLines(output_file, text, "{\"views\":[")
    println(text)
    publishData(text)

  }

  def extractData(results:ArrayBuffer[Any]):fd ={
    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Long,  ParArray[String]]]]
    try {
      val grouped = endResults.flatten.groupBy(f => f._1).mapValues(x => x.flatMap(_._2))
      val groupedNonIslands = grouped.filter(x => x._2.size > 1)
      val biggest = grouped.maxBy(_._2.size)._2.size
      val sorted = grouped.toArray.sortBy(_._2.size)(sortOrdering)//
      val top5 = sorted.map(x=>x._2.size).take(5)
      val total = grouped.size
      val totalWithoutIslands = groupedNonIslands.size
      val totalIslands = total - totalWithoutIslands
      val proportion = biggest.toFloat / grouped.map(x => x._2.size).sum
      val communities =  sorted.map(x=>x._2).take(if(top_c==0) sorted.length else top_c)//.values//.toArray
      fd(top5,total,totalIslands,proportion, communities)
    }  catch {
      case e: UnsupportedOperationException => fd(Array(0),0,0,0, Array(ArrayBuffer("0")))
    }
  }

  override def defineMaxSteps(): Int = 1000

}

case class fd(top5:Array[Int],total:Int,totalIslands:Int,proportion:Float, communities: Array[ArrayBuffer[String]])

