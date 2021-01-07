package com.raphtory.algorithms

import com.raphtory.api.Analyser
import com.raphtory.core.model.analysis.entityVisitors.{EdgeVisitor, VertexVisitor}

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.{ParArray, ParIterable, ParMap}

class MotifCounting(args: Array[String]) extends Analyser(args) { //args(delta)
  val delta: Long = if (args.isEmpty) throw new Exception else args.head.toLong //todo remove exception later
  val PROP = "weight"

  override def setup(): Unit = {
    view.getVertices.foreach { v =>
      val inc = v.getIncEdges
      val t_in = inc.flatMap(e => e.getHistory().keys).toArray.sorted
      val outc = v.getOutEdges
      val t_out = outc.flatMap(e => e.getHistory().keys).toArray.sorted
      //      println(v.ID() + "     " + (v.getIncEdges ++ v.getOutEdges).map(e => e.getHistory().size).sum)
      if (inc.nonEmpty & outc.nonEmpty) {
        val count1 = motifCounting(1, t_in, t_out)
        val count2 = motifCounting(2, t_in, t_out)
        v.setState("motifs", (count1,count2))
      }

      def motifCounting(mType: Int, in: Array[Long], out: Array[Long]): Long = {
        var (t_in, t_out) = (in.clone(), out.clone()) //todo <-- optimize
        mType match {
          case 1 => //todo change to fraction count
            t_in = t_in.reverse.dropWhile(_ >= t_out.last).reverse
            val tmp = t_in.groupBy(identity)
            t_in.distinct.foldLeft(0L) { case (a, t) =>
              t_out = t_out.filter(_ > t)
              a + t_out.count(_ <= t + delta) * tmp(t).length
            }
          case 2 =>
            val (mn, mx) = (t_in ++ t_out).foldLeft((t_in.head, t_in.head)) { case ((min, max), e) => (math.min(min, e), math.max(max, e)) }
//            var total = 0
            (for (dt <- mn to mx by delta) yield dt).count { dt =>
              //              if (checkActivity(v, dt, dt+delta)) total += 1 //todo for getting fractions
              val gamma1 = mean(inc.flatMap(getTimes(_, dt)).toParArray) < mean(outc.flatMap(getTimes(_, dt)).toParArray)
              val q = getProperties(inc, dt, PROP)
              val qq = getProperties(outc, dt, PROP)
//              val gamma2 = q.sum > qq.sum
              val gamma2 = getProperties(inc, dt, PROP).sum > getProperties(outc, dt, PROP).sum
              gamma1 & gamma2
            } // / total
          case _ => 0L
        }
      }
    }
  }

  def mean(a: ParArray[Long]): Double ={ //the fact i have to build this is maddening dont touch me
    if (a.nonEmpty) a.sum/a.length.toDouble else 0.0
  }
  def checkActivity(v: VertexVisitor, t1: Long, t2: Long): Boolean = {
    v.getInCEdgesBetween(t1, t2) ++ v.getOutEdgesBetween(t1, t2) nonEmpty
  }

  def getTimes(edge: EdgeVisitor, time: Long): Iterable[Long] = {
    edge.getHistory().filter { case (t, true) => t >= time & t < time + delta }.keys
  }

  def getProperties(edges: ParIterable[EdgeVisitor], time: Long, prop: String): ParIterable[Long] = {
    edges.map(e => getTimes(e, time).foldLeft(0L) { case (a, b) =>
      a + e.getPropertyValueAt(prop, b).getOrElse(0L).asInstanceOf[Long]
    })
  }

  override def analyse(): Unit = {}

  override def returnResults(): Any = {
    view.getVertices()
      .map(vertex => (vertex.ID(), vertex.getOrSetState[(Float,Float)]("motifs", (0.0, 0.0)))).toMap
  }

  override def processResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[ParMap[Long, (Float, Float)]]].flatten
    val count = endResults.sortBy(_._1).map(x => s""""${x._1}":${x._2}""")
    val nl = "\n"
    val text = s"""{"time":$timestamp,"motifs":{ $nl${count.mkString(",\n")} $nl},"viewTime":$viewCompleteTime}"""
    //  writeLines(output_file, text, "{\"views\":[")
    println(text)
  }

  override def processWindowResults(results: ArrayBuffer[Any], timestamp: Long, windowSize: Long, viewCompleteTime: Long): Unit = {
    val endResults = results.asInstanceOf[ArrayBuffer[ParMap[Long, Float]]].flatten
    val count = endResults.map(x => s""""${x._1}":${x._2}""")
    val text = s"""{"time":$timestamp,"windowsize":$windowSize,"motifs":{${count.mkString(",")}},"viewTime":$viewCompleteTime}"""
    //    writeLines(output_file, text, "{\"views\":[")
    println(text)
    publishData(text)
  }

  override def defineMaxSteps(): Int = 100
}
