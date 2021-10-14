//package com.raphtory.core.model.algorithm
//
//import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}
//import com.raphtory.core.model.graph.visitor.Edge
//
//import scala.collection.parallel.ParIterable
//import scala.math.Ordered.orderingToOrdered
//
//object MotifCounting {
//  def apply(top: Int, weight: String, delta: Long) =
//    new MotifCounting(Array(top, weight, delta).map(_.toString))
//}
//
//
//class MotifCounting(args: Array[String]) extends GraphAlgorithm {
//
//  val top: Int         = if (args.length == 0) 0 else args.head.toInt
//  val weight: String = if (args.length < 2) "weight" else args(1)
//  val delta: Long  = if (args.length < 3) throw new Exception else args(2).toLong
//
//  override def algorithm(graph: GraphPerspective): Unit = {
//
//  }
//
//  def motifCounting(mType: Int, inc: List[Edge], outc: List[Edge]): Double = {
//    var t_in= inc.map(e => e.history().foreach(event => event.time)).toArray.sorted
//    var t_out = outc.map(e => e.history().foreach(event => event.time)).toArray.sorted
//    val tEdges = t_in ++ t_out
//
//    if (inc.nonEmpty & outc.nonEmpty)
//      mType match {
//        case 1 =>
//          val total = nChoosek(tEdges.size) - tEdges.groupBy(identity).values.toArray.map(x => nChoosek(x.length)).sum
//          t_in = t_in.reverse.dropWhile(_ >= t_out.last).reverse
//          val tmp = t_in.groupBy(identity)
//          t_in.distinct.foldLeft(0L) {
//            case (a, t) =>
//              t_out = t_out.filter(_ > t)
//              a + t_out.count(_ <= t + delta) * tmp(t).length
//          } / total.toDouble
//        case 2 =>
//          val (mn, mx) = tEdges.foldLeft((tEdges.head, tEdges.head)) {case ((min, max), e) => (math.min(min, e), math.max(max, e))}
//          var total = 0
//          var count = 0
//          for (dt <- mn to mx by delta) {
//            if (checkActivity(inc ++ outc, dt, dt + delta)) total += 1
//            val gamma1 = mean(inc.flatMap(getTimes(_, dt)).map(_.toDouble).toArray) < mean(outc.flatMap(getTimes(_, dt)).map(_.toDouble).toArray)
//            val gamma2 = mean(getProperties(inc, dt, weight)) > mean(getProperties(outc, dt, weight))
//            count += (if (gamma1 & gamma2) 1 else 0)
//          }
//          count / total.toDouble
//        case _ => 0.0
//      }
//    else 0.0
//  }
//  def nChoosek(n: Long, k: Long = 2): Long         = if (k == 0L) 1L else (n * nChoosek(n - 1, k - 1)) / k
//  def mean(a: Array[Double]): Double =    if (a.nonEmpty) a.sum / a.length else 0.0 //the fact i have to build this is maddening dont touch me
//  def checkActivity(edges: ParIterable[Edge], t1: Long, t2: Long): Boolean =    edges.exists(e => e.history().exists(k => k._1 >= t1 && k._1 < t2)) //  IM: change this to range
//  def getTimes(edge: Edge, time: Long): Iterable[Long] =    edge.history().filter { case (t, true) => t >= time & t < time + delta }.keys
//  def getProperties(edges: ParIterable[Edge], time: Long, prop: String): Array[Double] =
//    edges.map(e => getTimes(e, time).foldLeft(0.0) {  case (a, b) => a + e.getPropertyValueAt(prop, b).getOrElse(0.0).asInstanceOf[Double] }).toArray
//
//
//}
