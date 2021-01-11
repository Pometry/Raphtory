package com.raphtory.algorithms

import com.raphtory.api.Analyser
import com.raphtory.core.model.analysis.entityVisitors.EdgeVisitor

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.{ParArray, ParIterable, ParMap}
import scala.reflect.io.{File, Path}

class MotifCounting(args: Array[String]) extends Analyser(args) { //todo better manage args
  val output_file = System.getenv().getOrDefault("MC_OUTPUT_PATH", "/home/tsunade/app/out.json").trim
  val Array(delta, step) = if (args.isEmpty) throw new Exception else args.map(_.toLong) //todo remove exception later
  val PROP: String       = System.getenv().getOrDefault("EDGE_PROPERTY", "weight").trim
  val nodeType = System.getenv().getOrDefault("NODE_TYPE", "Addr").trim

  override def setup(): Unit =
    view.getVertices.foreach { v =>
      val inc  = v.getIncEdges
      val outc = v.getOutEdges
//            println(v.ID() + "     " + (v.getIncEdges ++ v.getOutEdges).map(e => e.getHistory().size).sum)
      val count1 = motifCounting(1, inc, outc)
      val count2 = motifCounting(2, inc, outc)
      v.setState("motifs", (count1, count2))
    }

  override def analyse(): Unit = {}

  override def returnResults(): Any =
    view.getVertices().filter(v=> v.Type()==nodeType)
      .map(vertex => (vertex.ID(), vertex.getOrSetState[(Double, Double)]("motifs", (0.0, 0.0)))).toMap

  override def processResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[ParMap[Long, (Double, Double)]]].flatten
    val count      = endResults.sortBy(_._1).map(x => s""""${x._1}":${x._2}""")
//    val nl         = "\n"
//    val text       = s"""{"time":$timestamp,"motifs":{ $nl${count.mkString(",\n")} $nl},"viewTime":$viewCompleteTime}"""
    val text       = s"""{"time":$timestamp,"motifs":{ ${count.mkString(",")} },"viewTime":$viewCompleteTime}"""
    writerLine(output_file, text)
//    println(text)
  }

  override def processWindowResults(
      results: ArrayBuffer[Any],
      timestamp: Long,
      windowSize: Long,
      viewCompleteTime: Long
  ): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[ParMap[Long, Double]]].flatten
    val count = endResults.map(x => s""""${x._1}":${x._2}""")
    val text =
      s"""{"time":$timestamp,"windowsize":$windowSize,"motifs":{${count.mkString(",")}},"viewTime":$viewCompleteTime}"""
    writerLine(output_file, text)
//    println(text)
  }

  override def defineMaxSteps(): Int = 100

  def motifCounting(mType: Int, inc: ParIterable[EdgeVisitor], outc: ParIterable[EdgeVisitor]): Double = {
    var t_in   = inc.flatMap(e => e.getHistory().keys).toArray.sorted
    var t_out  = outc.flatMap(e => e.getHistory().keys).toArray.sorted
    val tEdges = t_in ++ t_out
    mType match {
      case 1 =>
        if (inc.nonEmpty & outc.nonEmpty) {
          val total = nChoosek(tEdges.length) - tEdges.groupBy(identity).values.toArray.map(x => nChoosek(x.length)).sum
          t_in = t_in.reverse.dropWhile(_ >= t_out.last).reverse
          val tmp = t_in.groupBy(identity)
          t_in.distinct.foldLeft(0L) {
            case (a, t) =>
              t_out = t_out.filter(_ > t)
              a + t_out.count(_ <= t + delta) * tmp(t).length
          } / total.toDouble
        } else 0.0
      case 2 =>
        if (inc.nonEmpty | outc.nonEmpty) {
          val (mn, mx) = tEdges.foldLeft((tEdges.head, tEdges.head)) {
            case ((min, max), e) => (math.min(min, e), math.max(max, e))
          }
          var total = 0
          (for (dt <- mn to mx by step) yield dt).count { dt =>
            if (checkActivity(inc ++ outc, dt, dt + delta)) total += 1
            val gamma1 = mean(inc.flatMap(getTimes(_, dt).map(_.toDouble)).toParArray) < mean(outc.flatMap(getTimes(_, dt).map(_.toDouble)).toParArray)
            val gamma2 = mean(getProperties(inc, dt, PROP).toParArray) > mean(getProperties(outc, dt, PROP).toParArray)
            gamma1 & gamma2
          } / total.toDouble
        } else 0.0
      case _ => 0
    }
  }
  def writerLine(path: String, line:String): Unit ={ Path(path).createFile().appendAll(line+"\n")  }
  def nChoosek(n: Long, k: Long = 2): Long = if (k == 0L) 1L else (n * nChoosek(n - 1, k - 1)) / k
  def mean(a: ParArray[Double]): Double =    if (a.nonEmpty) a.sum / a.length else 0.0 //the fact i have to build this is maddening dont touch me
  def checkActivity(edges: ParIterable[EdgeVisitor], t1: Long, t2: Long): Boolean = {    edges.exists(e => e.getHistory().exists(k => k._1 >= t1 && k._1 < t2))  }
  def getTimes(edge: EdgeVisitor, time: Long): Iterable[Long] =  edge.getHistory().filter { case (t, true) => t >= time & t < time + delta }.keys
  def getProperties(edges: ParIterable[EdgeVisitor], time: Long, prop: String): ParIterable[Double] =
    edges.map(e =>   getTimes(e, time).foldLeft(0.0) {  case (a, b) => a + e.getPropertyValueAt(prop, b).getOrElse(0.0).asInstanceOf[Double]  })
}
