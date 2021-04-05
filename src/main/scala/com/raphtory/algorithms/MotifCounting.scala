package com.raphtory.algorithms

import com.raphtory.core.analysis.api.Analyser
import com.raphtory.core.analysis.entity.Edge

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ParIterable
import scala.collection.parallel.mutable.ParMap
import scala.reflect.io.Path


/**
Description
  Returns the percentage of temporal motifs for every node in the graph. The algorithms identifies motifs specific to a node of the form
  2-edge-1-node motifs; It identifies two types of these motifs:
    Type-1: Detects motifs that exhibit incoming flow followed by outgoing flow in the form
              (n_1) -- t_k --> (n_2) -- t_{k+1} --> (n_3)
    Type-2: Detects attribute-based motifs that exhibit a non-negative balance while maintaining the temporal pattern
      of Type-1 i.e. on average, a larger incoming weight is followed by an lesser outgoing weight.

  Parameters
    top (Int)       – The number of nodes with highest number of motifs to return. (default: 0)
                        If not specified, Raphtory will return the motif count of all nodes that observe the motifs.
    weight (String) - Edge property (default: "weight"). To be specified for type-2 motifs.
    delta (Long)    - The size of the window in time to search for motifs where the length of the motif must be < delta.

  Returns
    total (Int)     – Number of nodes that exhibit a temporal motif.
    motifs (Map(Long, Map(Type, Double)))
                    – A dictionary of average number of motifs for Type1 and Type2 per node.
  **/

object MotifCounting {
  def apply(args: Array[String]): MotifCounting = new MotifCounting(args)
}

class MotifCounting(args: Array[String]) extends Analyser[Any](args) { //IM: better manage args
  //args = [delta, edge weight, top]
  val top: Int         = if (args.length == 0) 0 else args.head.toInt
  val weight: String = if (args.length < 2) "weight" else args(1)
  val delta: Long  = if (args.length < 3) throw new Exception else args(2).toLong //IM: remove exception later

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

  override def extractResults(results: Array[Any]): Any = {
    val endResults = results.asInstanceOf[ArrayBuffer[ParMap[Long, (Double, Double)]]].flatten
    val filtered      = endResults.filter(x=> (x._2._1>0)|(x._2._2>0)).map(x => s""""${x._1}":{"mc1":${x._2._1}, "mc2":${x._2._2}}""")
    val total         = filtered.length
    val count         = if (top == 0) filtered else filtered.take(top)
    val text          =
s"""{"total": $total, "motifs":{ ${count.mkString(",")} }}"""

    output_file match {
      case "" => println(text)
      case "mongo" => publishData(text)
      case _  => Path(output_file).createFile().appendAll(text + "\n")
    }
  }

  override def defineMaxSteps(): Int = 10

  def motifCounting(mType: Int, inc: ParIterable[Edge], outc: ParIterable[Edge]): Double = {
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
          val (mn, mx) = tEdges.foldLeft((tEdges.head, tEdges.head)) {case ((min, max), e) => (math.min(min, e), math.max(max, e))}
          var total = 0
          var count = 0
          for (dt <- mn to mx by delta) {
            if (checkActivity(inc ++ outc, dt, dt + delta)) total += 1
            val gamma1 = mean(inc.flatMap(getTimes(_, dt)).map(_.toDouble).toArray) < mean(outc.flatMap(getTimes(_, dt)).map(_.toDouble).toArray)
            val gamma2 = mean(getProperties(inc, dt, weight)) > mean(getProperties(outc, dt, weight))
            count += (if (gamma1 & gamma2) 1 else 0)
          }
          count / total.toDouble
        case _ => 0.0
      }
    else 0.0
  }
  def nChoosek(n: Long, k: Long = 2): Long         = if (k == 0L) 1L else (n * nChoosek(n - 1, k - 1)) / k
  def mean(a: Array[Double]): Double =    if (a.nonEmpty) a.sum / a.length else 0.0 //the fact i have to build this is maddening dont touch me
  def checkActivity(edges: ParIterable[Edge], t1: Long, t2: Long): Boolean =    edges.exists(e => e.getHistory().exists(k => k._1 >= t1 && k._1 < t2)) //  IM: change this to range
  def getTimes(edge: Edge, time: Long): Iterable[Long] =    edge.getHistory().filter { case (t, true) => t >= time & t < time + delta }.keys
  def getProperties(edges: ParIterable[Edge], time: Long, prop: String): Array[Double] =
    edges.map(e => getTimes(e, time).foldLeft(0.0) {  case (a, b) => a + e.getPropertyValueAt(prop, b).getOrElse(0.0).asInstanceOf[Double] }).toArray
}
