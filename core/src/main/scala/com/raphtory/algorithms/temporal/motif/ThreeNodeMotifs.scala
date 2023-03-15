package com.raphtory.algorithms.temporal.motif

import com.raphtory.algorithms.temporal.motif.ThreeNodeMotifs._
import com.raphtory.api.analysis.algorithm.GenericReduction
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.graphview.ReducedGraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table
import com.raphtory.internals.communication.SchemaProviderInstances._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class ThreeNodeMotifs {

  def arrayOp[T: ClassTag](a: Array[T], b: Array[T])(f: (T, T) => T): Array[T] = {
    assert(a.length == b.length)
    Array.tabulate(a.length)(i => f(a(i), b(i)))
  }
}
abstract class MotifCounter {

  val incoming: Int = 0
  val outgoing: Int = 1

  val dirsToMap: Seq[((Int, Int), Int)] = List((0, 0), (0, 1), (1, 0), (1, 1)).map(k => (k, 0))

  val preNodes: Array[Long]
  val postNodes: Array[Long]
  val preSum: Array[Long]
  val midSum: Array[Long]
  val postSum: Array[Long]

  def push(
      curNodes: Array[Long],
      curSum: Array[Long],
      curEdge: EdgeEvent
  ): Unit

  def pop(
      curNodes: Array[Long],
      curSum: Array[Long],
      curEdge: EdgeEvent
  ): Unit
  def processCurrent(curEdge: EdgeEvent): Unit

  def generateEvents(edges: List[(Long, Long, Long)]): Array[EdgeEvent]

  def execute(inputEdges: List[(Long, Long, Long)], delta: Long): Unit = {
    val L     = inputEdges.size
    if (L < 3)
      return
    val edges = generateEvents(inputEdges)
    var start = 0
    var end   = 0
    for (j <- 0 until L) {
      while (start < L && edges(start).time + delta < edges(j).time) {
        pop(preNodes, preSum, edges(start))
        start += 1
      }
      while (end < L && edges(end).time <= edges(j).time + delta) {
        push(postNodes, postSum, edges(end))
        end += 1
      }
      pop(postNodes, postSum, edges(j))
      processCurrent(edges(j))
      push(preNodes, preSum, edges(j))
    }
  }
}

class TriadMotifCounter(uid: Long, vid: Long, neighbours: Iterable[Long]) extends MotifCounter {
  import ThreeNodeMotifs.map2D
  import ThreeNodeMotifs.map3D

  val uorv: Map[Long, Int] = Map(uid -> 0, vid -> 1)

  val N: Int = neighbours.size

  val preNodes: Array[Long]    = Array.fill(4 * N)(0)
  val postNodes: Array[Long]   = Array.fill(4 * N)(0)
  val preSum: Array[Long]      = Array.fill(8)(0)
  val midSum: Array[Long]      = Array.fill(8)(0)
  val postSum: Array[Long]     = Array.fill(8)(0)
  val finalCounts: Array[Long] = Array.fill(8)(0)

  def push(
      curNodes: Array[Long],
      curSum: Array[Long],
      curEdge: EdgeEvent
  ): Unit = {
    val isUorV = curEdge.uorv; val nb = curEdge.nb; val dir = curEdge.dir
    if (nb != -1) {
      curSum(map3D(1 - isUorV, incoming, dir)) += curNodes(N * map2D(incoming, 1 - isUorV) + nb)
      curSum(map3D(1 - isUorV, outgoing, dir)) += curNodes(N * map2D(outgoing, 1 - isUorV) + nb)
      curNodes(N * map2D(dir, isUorV) + nb) += 1
    }
  }

  def pop(
      curNodes: Array[Long],
      curSum: Array[Long],
      curEdge: EdgeEvent
  ): Unit = {
    val isUorV = curEdge.uorv; val nb = curEdge.nb; val dir = curEdge.dir
    if (nb != -1) {
      curNodes(N * map2D(dir, isUorV) + nb) -= 1
      curSum(map3D(isUorV, dir, incoming)) -= curNodes(N * map2D(incoming, 1 - isUorV) + nb)
      curSum(map3D(isUorV, dir, outgoing)) -= curNodes(N * map2D(outgoing, 1 - isUorV) + nb)
    }
  }

  def processCurrent(curEdge: EdgeEvent): Unit = {
    val isUorV = curEdge.uorv; val nb = curEdge.nb; val dir = curEdge.dir
    if (nb != -1) {
      midSum(map3D(1 - isUorV, incoming, dir)) -= preNodes(N * map2D(incoming, 1 - isUorV) + nb)
      midSum(map3D(1 - isUorV, outgoing, dir)) -= preNodes(N * map2D(outgoing, 1 - isUorV) + nb)
      midSum(map3D(isUorV, dir, incoming)) += postNodes(N * map2D(incoming, 1 - isUorV) + nb)
      midSum(map3D(isUorV, dir, outgoing)) += postNodes(N * map2D(outgoing, 1 - isUorV) + nb)
    }
    else {
      var utov = 0
      if (isUorV == 1) utov = 1
      finalCounts(0) += midSum(map3D(utov, 0, 0)) + postSum(map3D(utov, 0, 1)) + preSum(map3D(1 - utov, 1, 1))
      finalCounts(4) += midSum(map3D(utov, 1, 0)) + postSum(map3D(1 - utov, 0, 1)) + preSum(map3D(1 - utov, 0, 1))
      finalCounts(2) += midSum(map3D(1 - utov, 0, 0)) + postSum(map3D(utov, 1, 1)) + preSum(map3D(1 - utov, 1, 0))
      finalCounts(6) += midSum(map3D(1 - utov, 1, 0)) + postSum(map3D(1 - utov, 1, 1)) + preSum(map3D(1 - utov, 0, 0))
      finalCounts(1) += midSum(map3D(utov, 0, 1)) + postSum(map3D(utov, 0, 0)) + preSum(map3D(utov, 1, 1))
      finalCounts(5) += midSum(map3D(utov, 1, 1)) + postSum(map3D(1 - utov, 0, 0)) + preSum(map3D(utov, 0, 1))
      finalCounts(3) += midSum(map3D(1 - utov, 0, 1)) + postSum(map3D(utov, 1, 0)) + preSum(map3D(utov, 1, 0))
      finalCounts(7) += midSum(map3D(1 - utov, 1, 1)) + postSum(map3D(1 - utov, 1, 0)) + preSum(map3D(utov, 0, 0))
    }
  }

  def generateEvents(edges: List[(Long, Long, Long)]): Array[EdgeEvent] = {
    val neighMap = new mutable.LongMap[Int](N + 2)
    neighbours.zipWithIndex.foreach { case (nb, i) => neighMap.put(nb, i) }
    neighMap.put(uid, -1)
    neighMap.put(vid, -1)
    edges.map { e =>
      if (e._2 == uid || e._2 == vid) EdgeEvent(neighMap(e._1), incoming, uorv(e._2), e._3)
      else EdgeEvent(neighMap(e._2), outgoing, uorv(e._1), e._3)
    }.toArray
  }

  def getCounts: Array[Long] =
    finalCounts
}

class StarMotifCounter(vid: Long, neighbours: Iterable[Long]) extends MotifCounter {
  import ThreeNodeMotifs.map2D
  import ThreeNodeMotifs.map3D

  val N: Int = neighbours.size

  val preNodes: Array[Long]  = Array.fill(2 * neighbours.size)(0)
  val postNodes: Array[Long] = Array.fill(2 * neighbours.size)(0)

  val preSum: Array[Long]  = Array.fill(8)(0)
  val midSum: Array[Long]  = Array.fill(8)(0)
  val postSum: Array[Long] = Array.fill(8)(0)

  val countPre: Array[Long]  = Array.fill(8)(0)
  val countMid: Array[Long]  = Array.fill(8)(0)
  val countPost: Array[Long] = Array.fill(8)(0)

  override def push(
      curNodes: Array[Long],
      curSum: Array[Long],
      curEdge: EdgeEvent
  ): Unit = {
    val dir = curEdge.dir; val nb = curEdge.nb
    curSum(map2D(incoming, dir)) += curNodes(incoming * N + nb)
    curSum(map2D(outgoing, dir)) += curNodes(outgoing * N + nb)
    curNodes(dir * N + nb) += 1
  }

  override def pop(
      curNodes: Array[Long],
      curSum: Array[Long],
      curEdge: EdgeEvent
  ): Unit = {
    val dir = curEdge.dir; val nb = curEdge.nb
    curNodes(dir * N + nb) -= 1
    curSum(map2D(dir, incoming)) -= curNodes(incoming * N + nb)
    curSum(map2D(dir, outgoing)) -= curNodes(outgoing * N + nb)
  }

  override def processCurrent(curEdge: EdgeEvent): Unit = {
    val dir = curEdge.dir
    val nb  = curEdge.nb
    midSum(map2D(incoming, dir)) -= preNodes(incoming * N + nb)
    midSum(map2D(outgoing, dir)) -= preNodes(outgoing * N + nb)

    for ((dir1, dir2) <- dirs2D) {
      countPre(map3D(dir1, dir2, dir)) += preSum(map2D(dir1, dir2))
      countPost(map3D(dir, dir1, dir2)) += postSum(map2D(dir1, dir2))
      countMid(map3D(dir1, dir, dir2)) += midSum(map2D(dir1, dir2))
    }

    midSum(map2D(dir, incoming)) += postNodes(incoming * N + nb)
    midSum(map2D(dir, outgoing)) += postNodes(outgoing * N + nb)
  }

  def generateEvents(edges: List[(Long, Long, Long)]): Array[EdgeEvent] = {
    val neighMap = new mutable.LongMap[Int](N)
    neighbours.zipWithIndex.foreach { case (nb, i) => neighMap.put(nb, i) }
    edges.map { e =>
      if (e._2 == vid) EdgeEvent(neighMap(e._1), incoming, -1, e._3) else EdgeEvent(neighMap(e._2), outgoing, -1, e._3)
    }.toArray
  }

  def getCounts: Array[Long] = countPre ++ countMid ++ countPost
}

class TwoNodeMotifs(vid: Long) {

  val incoming: Int = 0
  val outgoing: Int = 1

  val count1d: Array[Long] = Array.fill(2)(0)
  val count2d: Array[Long] = Array.fill(4)(0)
  val count3d: Array[Long] = Array.fill(8)(0)

  def execute(inputEdges: Array[(Long, Long, Long)], delta: Long): Unit = {
    var start = 0
    val edges = inputEdges
    for (end <- edges.indices) {
      while (edges(start)._3 + delta < edges(end)._3) {
        decrementCounts(edges(start))
        start += 1
      }
      incrementCounts(edges(end))
    }
  }

  def decrementCounts(edge: (Long, Long, Long)): Unit = {
    val dir = if (edge._2 == vid) incoming else outgoing
    count1d(dir) -= 1
    count2d(map2D(dir, incoming)) -= count1d(incoming)
    count2d(map2D(dir, outgoing)) -= count1d(outgoing)
  }

  def incrementCounts(edge: (Long, Long, Long)): Unit = {
    val dir = if (edge._2 == vid) incoming else outgoing

    for ((dir1, dir2) <- dirs2D)
      count3d(map3D(dir1, dir2, dir)) += count2d(map2D(dir1, dir2))

    count2d(map2D(incoming, dir)) += count1d(incoming)
    count2d(map2D(outgoing, dir)) += count1d(outgoing)

    count1d(dir) += 1
  }

  def getCounts: Array[Long] =
    count3d
}

case class RequestEdges(sendTo: Long, dst: Long)
case class EdgeEvent(nb: Int, dir: Int, uorv: Int, time: Long)

object ThreeNodeMotifs {

  def map2D(d1: Int, d2: Int): Int = 2 * d1 + d2

  def map3D(d1: Int, d2: Int, d3: Int): Int = 4 * d1 + 2 * d2 + d3

  val dirs2D: List[(Int, Int)] = List((0, 0), (0, 1), (1, 0), (1, 1))

  def getTriCountsPretty[T: Numeric](triCounts: Array[T]): Map[String, T] = {
    val prettyCounts = mutable.Map[String, T]()
    prettyCounts.put("i --> j, k --> j, i --> k", triCounts(0))
    prettyCounts.put("i --> j, k --> i, j --> k", triCounts(1))
    prettyCounts.put("i --> j, j --> k, i --> k", triCounts(2))
    prettyCounts.put("i --> j, i --> k, j --> k", triCounts(3))
    prettyCounts.put("i --> j, k --> j, k --> i", triCounts(4))
    prettyCounts.put("i --> j, k --> i, k --> j", triCounts(5))
    prettyCounts.put("i --> j, j --> k, k --> i", triCounts(6))
    prettyCounts.put("i --> j, i --> k, k --> j", triCounts(7))
    prettyCounts.toMap
  }

  def get2NodeCountsPretty[T: Numeric](counts: Array[T]): Map[String, T] = {
    val prettyCounts = mutable.Map[String, T]()
    prettyCounts.put("III", counts(0))
    prettyCounts.put("IIO", counts(1))
    prettyCounts.put("IOI", counts(2))
    prettyCounts.put("IOO", counts(3))
    prettyCounts.put("OII", counts(4))
    prettyCounts.put("OIO", counts(5))
    prettyCounts.put("OOI", counts(6))
    prettyCounts.put("OOO", counts(7))
    prettyCounts.toMap
  }

  def get2NodeCountsWithoutRepeats[T: Numeric](counts: Array[T]): Map[String, T] =
    get2NodeCountsPretty(counts).map { case (k, v) => ("2NODE-" + k, v) }

  def getStarCountsPretty[T: Numeric](counts: Array[T]): Map[String, T] = {
    val preMap  = get2NodeCountsPretty(counts.slice(0, 8)).map { case (k, v) => ("STAR-PRE-" + k, v) }
    val midMap  = get2NodeCountsPretty(counts.slice(8, 16)).map { case (k, v) => ("STAR-MID-" + k, v) }
    val postMap = get2NodeCountsPretty(counts.slice(16, 24)).map { case (k, v) => ("STAR-POST-" + k, v) }
    preMap ++ midMap ++ postMap
  }
}
