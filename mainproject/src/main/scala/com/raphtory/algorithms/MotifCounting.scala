package com.raphtory.algorithms

import com.raphtory.api.Analyser
import com.raphtory.core.model.analysis.entityVisitors.EdgeVisitor

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.{ParArray, ParIterable, ParMap}
import scala.reflect.io.Path

object MotifCounting {
  def apply(args: Array[String]): MotifCounting = new MotifCounting(args)
}

class MotifCounting(args: Array[String]) extends Analyser(args) { //todo: IM: better manage args
//args = [delta, edge weight]
  val delta: Long  = if (args.isEmpty) throw new Exception else args.head.toLong //todo: IM: remove exception later
  val PROP: String = if (args.length < 2) "weight" else args(1)

  val output_file: String = System.getenv().getOrDefault("MC_OUTPUT_PATH", "").trim
  val nodeType: String    = System.getenv().getOrDefault("NODE_TYPE", "").trim

  override def setup(): Unit = {}

  override def analyse(): Unit = {}

  override def returnResults(): Any =
    view
      .getVertices()
      .filter(v => v.Type() == nodeType)
      .map { vertex =>
        val inc    = vertex.getIncEdges
        val outc   = vertex.getOutEdges
        val count1 = motifCounting(1, inc, outc)
        val count2 = motifCounting(2, inc, outc)
        (vertex.ID(), (count1, count2))
      }
      .toMap

  override def processResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[ParMap[Long, (Double, Double)]]].flatten
    val count      = endResults.sortBy(_._1).map(x => s""""${x._1}":{"mc1":${x._2._1}, "mc2":${x._2._2}}""")
//    val nl         = "\n"
//    val text       = s"""{"time":$timestamp,"motifs":{ $nl${count.mkString(",\n")} $nl},"viewTime":$viewCompleteTime}"""
    val text = s"""{"time":$timestamp,"motifs":{ ${count.mkString(",")} },"viewTime":$viewCompleteTime}"""
    if (output_file.nonEmpty) Path(output_file).createFile().appendAll(text + "\n")
    else println(text)
  }

  override def processWindowResults(
      results: ArrayBuffer[Any],
      timestamp: Long,
      windowSize: Long,
      viewCompleteTime: Long
  ): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[ParMap[Long, (Double, Double)]]].flatten
    val count      = endResults.sortBy(_._1).map(x => s""""${x._1}":{"mc1":${x._2._1}, "mc2":${x._2._2}}""")
    val text = s"""{"time":$timestamp,"windowsize":$windowSize,"motifs":{ ${count
      .mkString(",")} },"viewTime":$viewCompleteTime}"""
    if (output_file.nonEmpty) Path(output_file).createFile().appendAll(text + "\n")
    else println(text)
  }

  override def defineMaxSteps(): Int = 100

  def motifCounting(mType: Int, inc: ParIterable[EdgeVisitor], outc: ParIterable[EdgeVisitor]): Double = {
    var t_in   = inc.flatMap(e => e.getHistory().keys).toArray.sorted
    var t_out  = outc.flatMap(e => e.getHistory().keys).toArray.sorted
    val tEdges = t_in ++ t_out

    if (inc.nonEmpty & outc.nonEmpty)
      mType match {
        case 1 =>
          val total = nChoosek(tEdges.length) - tEdges.groupBy(identity).values.toArray.map(x => nChoosek(x.length)).sum
          t_in = t_in.reverse.dropWhile(_ >= t_out.last).reverse
          val tmp = t_in.groupBy(identity)
          t_in.distinct.foldLeft(0L) {
            case (a, t) =>
              t_out = t_out.filter(_ > t)
              a + t_out.count(_ <= t + delta) * tmp(t).length
          } / total.toDouble
        case 2 =>
          val (mn, mx) = tEdges.foldLeft((tEdges.head, tEdges.head)) {    case ((min, max), e) => (math.min(min, e), math.max(max, e))          }
          var total = 0
          (for (dt <- mn to mx by delta) yield dt).count { dt =>
            if (checkActivity(inc ++ outc, dt, dt + delta)) total += 1
            val gamma1 = mean(inc.flatMap(getTimes(_, dt)).toArray) < mean(outc.flatMap(getTimes(_, dt)).toArray)
            val gamma2 = mean(getProperties(inc, dt, PROP)) > mean(getProperties(outc, dt, PROP))
            gamma1 & gamma2
          } / total.toDouble
        case _ => 0.0
      }
    else 0.0
  }
  def nChoosek(n: Long, k: Long = 2): Long         = if (k == 0L) 1L else (n * nChoosek(n - 1, k - 1)) / k
  def mean(a: Array[Long]): Double =    if (a.nonEmpty) a.sum / a.length.toDouble else 0.0 //the fact i have to build this is maddening dont touch me
  def checkActivity(edges: ParIterable[EdgeVisitor], t1: Long, t2: Long): Boolean =    edges.exists(e => e.getHistory().exists(k => k._1 >= t1 && k._1 < t2)) // TODO: IM: change this to range
  def getTimes(edge: EdgeVisitor, time: Long): Iterable[Long] =    edge.getHistory().filter { case (t, true) => t >= time & t < time + delta }.keys
  def getProperties(edges: ParIterable[EdgeVisitor], time: Long, prop: String): Array[Long] =
    edges.map(e => getTimes(e, time).foldLeft(0L) {  case (a, b) => a + e.getPropertyValueAt(prop, b).getOrElse(0L).asInstanceOf[Long] }).toArray
}
