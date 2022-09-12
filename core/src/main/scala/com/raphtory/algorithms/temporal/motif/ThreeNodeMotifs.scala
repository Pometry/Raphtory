package com.raphtory.algorithms.temporal.motif

import com.raphtory.algorithms.temporal.motif.ThreeNodeMotifs.{dirs2D, get2NodeCountsWithoutRepeats, getStarCountsPretty, getTriCountsPretty, map2D, map3D}
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.algorithm.GenericReduction
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.graphview.ReducedGraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

import scala.collection.mutable.LongMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  *  {s}`ThreeNodeMotifs(delta:Long, graphWide:Boolean=true, prettyPrint:Boolean=true)`
  *    : Count occurrences of three-edge up-to-three-node temporal motifs in the network. For counts per vertex, see LocalThreeNodeMotifs.
  *
  *  The algorithm is very based on that in "Motifs in Temporal Networks". An option is given to return results as a hashmap with labels or as an array which is easier for post-processing.
  *
  *  ## Motifs
  *
  *  ### Stars
  *
  *  There are three classes (in the order they are outputted) of star motif on three nodes based on the switching behaviour of the edges between the two leaf nodes.
  *
  *   - PRE: Stars of the form i<->j, i<->j, i<->k (ie two interactions with leaf j followed by one with leaf k)
  *   - MID: Stars of the form i<->j, i<->k, i<->j (ie switching interactions from leaf j to leaf k, back to j again)
  *   - POST: Stars of the form i<->j, i<->k, i<->k (ie one interaction with leaf j followed by two with leaf k)
  *
  *  Within each of these classes is 8 motifs depending on the direction of the first to the last edge -- incoming "I" or outgoing "O".
  *  These are enumerated in the order III, IIO, IOI, IOO, OII, OIO, OOI, OOO (like binary with "I"-0 and "O"-1).
  *
  *  ### Two node motifs
  *
  *  Also included are two node motifs, of which there are 8 when counted from the perspective of each vertex. These are characterised by the direction of each edge, enumerated
  *  in the above order. Note that for the global graph counts, each motif is counted in both directions (a single III motif for one vertex is an OOO motif for the other vertex).
  *
  *  ### Triangles
  *
  *  There are 8 triangle motifs, below is the order in which they appear in the returned array:
  *
  *   1. i --> j, k --> j, i --> k
  *   2. i --> j, k --> i, j --> k
  *   3. i --> j, j --> k, i --> k
  *   4. i --> j, i --> k, j --> k
  *   5. i --> j, k --> j, k --> i
  *   6. i --> j, k --> i, k --> j
  *   7. i --> j, j --> k, k --> i
  *   8. i --> j, i --> k, k --> j
  *
  * ## States
  *  {s}`starCounts: Array[Int]`
  *    : Three-node star motif counts stored as an array (see indices above)
  *  {s}`twoNodeCounts: Array[Int]`
  *    : Two-node motif counts stored as an array (see indices above)
  *  {s}`triCounts: Array[Int]`
  *    : Triangle motif counts stored as an array (see indices above)
  */

class ThreeNodeMotifs(delta: Long = 3600, graphWide: Boolean = false, prettyPrint: Boolean = true)
        extends GenericReduction {

  override def apply(graph: GraphPerspective): graph.ReducedGraph = {
    graph.reducedView
      .setGlobalState { state =>
        state.newConcurrentAccumulator[Array[Long]](
                "twoNodeCounts",
                Array.fill(8)(0L),
                retainState = true,
                arrayOp(_, _)(_ + _)
        )
        state.newConcurrentAccumulator[Array[Long]](
                "triCounts",
                Array.fill(8)(0L),
                retainState = true,
                arrayOp(_, _)(_ + _)
        )
        state.newConcurrentAccumulator[Array[Long]](
                "starCounts",
                Array.fill(24)(0L),
                retainState = true,
                arrayOp(_, _)(_ + _)
        )
      }
      .step { v =>
        // Ensure no self loops are counted within motifs
        val neighbours = v.neighbours.filter(_ != v.ID).toSet
        neighbours.foreach { nb =>
          v.messageVertex(nb, (v.ID, neighbours))
        }
      }
      // this step gets a list of (static) edges that form the opposite edge of a triangle with a node. this is ordered
      // smallest id first.
      .step { v =>
        // Ensure no self loops are counted within motifs
        val neighbours = v.neighbours.filter(_ != v.ID).toSet
        val queue      = v.messageQueue[(v.IDType, Set[v.IDType])]
        queue.foreach {
          // Pick a representative node who will hold all the exploded edge info.
          case (nb, friendsOfFriend) =>
            friendsOfFriend.intersect(neighbours).foreach { w =>
              // largest id node sends to the smallest id node the exploded edges.
              if (nb < w && w.max(nb).max(v.ID) == v.ID)
                // message the edge and the size of the edge history
                v.messageVertex(nb, (v.ID.min(w), v.ID.max(w), v.explodedEdge(w).getOrElse(List()).size))
            }
        }
      }
      // in this step only the smallest id node in the triangle is looking after the process of finding emax.
      .step { v =>
        val opEdgeSizes = v.messageQueue[(Long, Long, Int)]
        opEdgeSizes.foreach { edge =>
          val triMap: mutable.Map[(Long, Long), Int] = mutable.Map()
          // already ordered in id size from u to w.
          val (u, w, size)                           = (edge._1, edge._2, edge._3)
          triMap.put((u, w), size)
          triMap.put((v.ID, u), v.explodedEdge(u).getOrElse(List()).size)
          triMap.put((v.ID, w), v.explodedEdge(w).getOrElse(List()).size)
          // order first by size then by id1 then id2 to ensure uniqueness.
          val eMax                                   = triMap.maxBy(x => (x._2, x._1._1, x._1._2))._1
          if (eMax._1 == v.ID) {
            // request the history of the other edges
            v.messageVertex(eMax._2, RequestEdges(v.ID, if (eMax._2 == u) w else u))
            val appendedEdge = v.getEdge(eMax._2).minBy(_.src)
            v.explodedEdge(if (eMax._2 == u) w else u)
              .getOrElse(List())
              .foreach(e => appendedEdge.appendToState[(Long, Long, Long)]("a_e", (e.src, e.dst, e.timestamp)))
          }
          else
            // send to the vertex looking after eMax
            v.messageVertex(u, (w, v.explodedEdge(w).getOrElse(List()).map(e => (e.src, e.dst, e.timestamp))))
        }
      }
      // Either return edges to the requested vertex or set exploded edges received as edge state
      .step { v =>
        val queue = v.messageQueue[Any]
        queue.foreach {
          case RequestEdges(sendTo, dst)               =>
            v.messageVertex(sendTo, (v.ID, v.explodedEdge(dst).getOrElse(List()).map(e => (e.src, e.dst, e.timestamp))))
          case edges: (Long, List[(Long, Long, Long)]) =>
            edges._2.foreach(e => v.getEdge(edges._1).minBy(_.src).appendToState[(Long, Long, Long)]("a_e", e))
            val dst          = if (edges._2.head._1 == edges._1) edges._2.head._2 else edges._2.head._1
            val appendedEdge = v.getEdge(edges._1).minBy(_.src)
            v.explodedEdge(dst)
              .getOrElse(List())
              .foreach(e => appendedEdge.appendToState[(Long, Long, Long)]("a_e", (e.src, e.dst, e.timestamp)))
        }
      }
      .step { v =>
        v.messageQueue[(Long, List[(Long, Long, Long)])].foreach { edges =>
          edges._2.foreach { e =>
            v.getEdge(edges._1)
              .minBy(_.src)
              .appendToState[(Long, Long, Long)]("a_e", e)
          }
        }
      }
      .step { (v, state) =>
        v("triCounts") = Array.fill(8)(0L)
        v.neighbours.filter(_ != v.ID).foreach { nb =>
          val edge = v.getEdge(nb).minBy(_.src)
          val a_e  = edge.getStateOrElse[ArrayBuffer[(Long, Long, Long)]]("a_e", ArrayBuffer())
          edge.clearState("a_e")
          if (a_e.nonEmpty) {
            // Here we sort the edges not only by a timestamp but an additional index meaning that we obtain consistent results
            // for motif edges with the same timestamp
            val inputEdges = a_e
              .appendedAll(
                      v.explodedEdge(nb)
                        .getOrElse(List())
                        .map(e => (e.src, e.dst, e.timestamp))
              )
              .toList
              .sortBy(x => (x._3, x._1, x._2))
            val opNeighbours = inputEdges.foldRight[Set[Long]](Set())((edge, nbset) => nbset.union(Set(edge._1,edge._2))).removedAll(List(v.ID,nb))
            val mc         = new TriadMotifCounter(v.ID, nb, opNeighbours)
            mc.execute(inputEdges, delta)
            val curVal     = v.getState[Array[Long]]("triCounts")
            v("triCounts") = arrayOp(curVal,mc.getCounts)(_+_)
            state("triCounts") += mc.getCounts.map(_.toLong)
          }
        }
      }
      .step { (v, state) =>
        val mc                 = new StarMotifCounter(v.ID, v.neighbours.filter(_ != v.ID))
        // Here we sort the edges not only by a timestamp but an additional index meaning that we obtain consistent results
        // for motif edges with the same timestamp
        mc.execute(v.explodeAllEdges().map(e => (e.src, e.dst, e.timestamp)).sortBy(x => (x._3, x._1, x._2)), delta)
        val counts: Array[Long] = mc.getCounts
        var twoNodeCounts      = Array.fill(8)(0)
        v.neighbours.filter(_ != v.ID).foreach { vid =>
          val mc2node = new TwoNodeMotifs(v.ID)
          // Here we sort the edges not only by a timestamp but an additional index meaning that we obtain consistent results
          // for motif edges with the same timestamp
          mc2node.execute(
                  v.explodedEdge(vid).getOrElse(List()).map(e => (e.src, e.dst, e.timestamp)) sortBy (x =>
                    (x._3, x._1, x._2)
                  ),
                  delta
          )
          val twoNC   = mc2node.getCounts
          for (i <- counts.indices)
            counts.update(i, counts(i) - twoNC(i % 8))
          twoNodeCounts = arrayOp(twoNodeCounts,twoNC)(_+_)
        }
        v("twoNodeCounts") = twoNodeCounts
        state("twoNodeCounts") += twoNodeCounts.map(_.toLong)
        v("starCounts") = counts
        state("starCounts") += counts.map(_.toLong)
      }
  }

  @inline
  def arrayOp[T: ClassTag](a: Array[T], b: Array[T])(f: (T, T) => T): Array[T] = {
    assert(a.length == b.length)
    Array.tabulate(a.length)(i => f(a(i), b(i)))
  }

  override def tabularise(graph: ReducedGraphPerspective): Table =
    if (!graphWide)
      if (prettyPrint)
        graph.select(vertex =>
          Row(
                  vertex.name,
                  getStarCountsPretty(vertex.getState[Array[Int]]("starCounts")),
                  get2NodeCountsWithoutRepeats(vertex.getState[Array[Int]]("twoNodeCounts")),
                  getTriCountsPretty(vertex.getState[Array[Int]]("triCounts"))
          )
        )
      else
        graph.select(vertex =>
          Row(
                  vertex.name,
                  (vertex.getState[Array[Int]]("starCounts") ++ vertex.getState[Array[Int]]("twoNodeCounts") ++ vertex
                    .getState[Array[Int]]("triCounts")).mkString("(", ";", ")")
          )
        )
    else if (prettyPrint)
      graph.globalSelect(state =>
        Row(
                getStarCountsPretty(state[Array[Long], Array[Long]]("starCounts").value),
                get2NodeCountsWithoutRepeats(state[Array[Int], Array[Int]]("twoNodeCounts").value),
                getTriCountsPretty(state[Array[Int], Array[Int]]("triCounts").value)
        )
      )
    else
      graph.globalSelect(state =>
        Row(
                (state[Array[Long], Array[Long]]("starCounts").value ++ state[Array[Int], Array[Int]](
                        "twoNodeCounts"
                ).value ++ state[Array[Int], Array[Int]]("triCounts").value).mkString("(", ";", ")")
        )
      )
}

abstract class MotifCounter {

  val incoming: Int = 0
  val outgoing: Int = 1

  val dirsToMap: Seq[((Int, Int), Int)]        = List((0, 0), (0, 1), (1, 0), (1, 1)).map(k => (k, 0))

  val preNodes:Array[Long]
  val postNodes:Array[Long]
  val preSum:Array[Long]
  val midSum:Array[Long]
  val postSum:Array[Long]

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

  def generateEvents(edges: List[(Long,Long,Long)]) : List[EdgeEvent]

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

class TriadMotifCounter(uid: Long, vid: Long, neighbours:Iterable[Long]) extends MotifCounter {
import ThreeNodeMotifs.{map2D,map3D}

  val uorv: Map[Long, Int] = Map(uid -> 0, vid -> 1)

  val N: Int = neighbours.size

  val preNodes:Array[Long] = Array.fill(4*N)(0)
  val postNodes:Array[Long] = Array.fill(4*N)(0)
  val preSum:Array[Long] = Array.fill(8)(0)
  val midSum:Array[Long] = Array.fill(8)(0)
  val postSum:Array[Long] = Array.fill(8)(0)
  val finalCounts:Array[Long] = Array.fill(8)(0)

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
    if(nb!= -1) {
      curNodes(N * map2D(dir, isUorV) + nb) -= 1
      curSum(map3D(isUorV, dir, incoming)) -= curNodes(N * map2D(incoming, 1 - isUorV) + nb)
      curSum(map3D(isUorV, dir, outgoing)) -= curNodes(N * map2D(outgoing, 1 - isUorV) + nb)
    }
  }

  def processCurrent(curEdge: EdgeEvent): Unit = {
    val isUorV = curEdge.uorv; val nb = curEdge.nb; val dir = curEdge.dir
    if (nb != -1)
    {
      midSum(map3D(1 - isUorV, incoming, dir)) -= preNodes(N*map2D(incoming, 1 - isUorV) + nb)
      midSum(map3D(1 - isUorV, outgoing, dir)) -= preNodes(N*map2D(outgoing, 1 - isUorV) + nb)
      midSum(map3D(isUorV, dir, incoming)) += postNodes(N*map2D(incoming, 1 - isUorV) + nb)
      midSum(map3D(isUorV, dir, outgoing)) += postNodes(N*map2D(outgoing, 1 - isUorV) + nb)
    }
    else {
      var utov = 0
      if (isUorV == 1) {utov = 1}
      finalCounts(0) += midSum(map3D(utov, 0, 0)) + postSum(map3D(utov, 0, 1)) + preSum(map3D(1 - utov, 1, 1))
      finalCounts(4) += midSum(map3D(utov, 1, 0)) + postSum(map3D(1 - utov, 0, 1)) + preSum(map3D(1 - utov, 0, 1))
      finalCounts(2) += midSum(map3D(1 - utov, 0, 0)) + postSum(map3D(utov, 1, 1)) + preSum(map3D(1 - utov, 1, 0))
      finalCounts(6) += midSum(map3D(1 - utov, 1, 0)) + postSum(map3D(1 - utov, 1, 1)) + preSum(map3D(1 - utov, 0, 0))
      finalCounts(1) += midSum(map3D(utov, 0, 1)) + postSum(map3D(utov, 0, 0)) + preSum(map3D(utov, 1, 1))
      finalCounts(5) += midSum(map3D(utov, 1, 1)) + postSum(map3D(1 - utov, 0, 0)) + preSum(map3D(utov, 0, 1))
      finalCounts(3) += midSum(map3D(1 - utov, 0, 1)) + postSum(map3D(utov, 1, 0)) + preSum(map3D(utov, 1, 0))
      finalCounts(7) += midSum(map3D(1 - utov, 1, 1)) + postSum(map3D(1 - utov, 1, 0)) + preSum(map3D(utov, 0, 0))
    } }

  def generateEvents(edges: List[(Long, Long, Long)]) : List[EdgeEvent] = {
    val neighMap = new mutable.LongMap[Int](N+2)
    neighbours.zipWithIndex.foreach{case (nb, i) => neighMap.put(nb,i)}
    neighMap.put(uid,-1)
    neighMap.put(vid,-1)
    edges.map{ e =>
      if (e._2 == uid || e._2 == vid) EdgeEvent(neighMap(e._1),incoming,uorv(e._2),  e._3) else EdgeEvent(neighMap(e._2),outgoing,uorv(e._1), e._3)
    }
  }

  def getCounts: Array[Long] =
    finalCounts
}

class StarMotifCounter(vid: Long, neighbours:Iterable[Long]) extends MotifCounter {
  import ThreeNodeMotifs.{map2D,map3D}

  val N: Int = neighbours.size

  val preNodes:Array[Long] = Array.fill(2*neighbours.size)(0)
  val postNodes:Array[Long] = Array.fill(2*neighbours.size)(0)

  val preSum:Array[Long] = Array.fill(8)(0)
  val midSum:Array[Long] = Array.fill(8)(0)
  val postSum:Array[Long] = Array.fill(8)(0)

  val countPre: Array[Long] = Array.fill(8)(0)
  val countMid: Array[Long] = Array.fill(8)(0)
  val countPost: Array[Long] = Array.fill(8)(0)

  override def push(
      curNodes: Array[Long],
      curSum: Array[Long],
      curEdge: EdgeEvent
  ): Unit = {
    val dir = curEdge.dir; val nb = curEdge.nb
    curSum(map2D(incoming,dir)) += curNodes(incoming*N + nb)
    curSum(map2D(outgoing,dir)) += curNodes(outgoing*N + nb)
    curNodes(dir*N + nb) += 1
  }

  override def pop(
                    curNodes: Array[Long],
                    curSum: Array[Long],
                    curEdge: EdgeEvent
  ): Unit = {
    val dir = curEdge.dir; val nb = curEdge.nb
    curNodes(dir*N + nb) -= 1
    curSum(map2D(dir,incoming)) -= curNodes(incoming*N + nb)
    curSum(map2D(dir,outgoing)) -= curNodes(outgoing*N + nb)
  }

  override def processCurrent(curEdge: EdgeEvent): Unit = {
    val dir = curEdge.dir
    val nb = curEdge.nb
    midSum(map2D(incoming,dir)) -= preNodes(incoming*N + nb)
    midSum(map2D(outgoing,dir)) -= preNodes(outgoing*N + nb)

    for ((dir1,dir2) <- dirs2D) {
      countPre(map3D(dir1,dir2,dir)) += preSum(map2D(dir1,dir2))
      countPost(map3D(dir,dir1,dir2)) += postSum(map2D(dir1,dir2))
      countMid(map3D(dir1,dir,dir2)) += midSum(map2D(dir1,dir2))
    }

    midSum(map2D(dir,incoming)) += postNodes(incoming*N + nb)
    midSum(map2D(dir,outgoing)) += postNodes(outgoing*N + nb)
  }

  def generateEvents(edges: List[(Long, Long, Long)]) : List[EdgeEvent] = {
    val neighMap = new mutable.LongMap[Int](N)
    neighbours.zipWithIndex.foreach{case (nb, i) => neighMap.put(nb,i)}
    edges.map{ e =>
      if (e._2 == vid) EdgeEvent(neighMap(e._1),incoming, -1, e._3) else EdgeEvent(neighMap(e._2),outgoing,-1, e._3)
    }
  }

  def getCounts: Array[Long] = countPre++countMid++countPost
}

class TwoNodeMotifs(vid: Long) {

  val incoming: Int = 0
  val outgoing: Int = 1

  val count1d: Array[Int] = Array.fill(2)(0)
  val count2d: Array[Int] = Array.fill(4)(0)
  val count3d: Array[Int] = Array.fill(8)(0)

  def execute(edges: List[(Long, Long, Long)], delta: Long): Unit = {
    var start = 0
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
    count2d(map2D(dir,incoming)) -= count1d(incoming)
    count2d(map2D(dir,outgoing)) -= count1d(outgoing)
  }

  def incrementCounts(edge: (Long, Long, Long)): Unit = {
    val dir = if (edge._2 == vid) incoming else outgoing

    for ((dir1, dir2) <- dirs2D) {
      count3d(map3D(dir1,dir2,dir)) += count2d(map2D(dir1,dir2))
    }

    count2d(map2D(incoming,dir)) += count1d(incoming)
    count2d(map2D(outgoing,dir)) += count1d(outgoing)

    count1d(dir) += 1
  }

  def getCounts: Array[Int] =
    count3d
}

case class RequestEdges(sendTo: Long, dst: Long)
case class EdgeEvent(nb:Int, dir:Int, uorv:Int, time:Long)

object ThreeNodeMotifs {

  def apply(delta: Long = 3600, graphWide: Boolean = false, prettyPrint: Boolean = true) =
    new ThreeNodeMotifs(delta, graphWide, prettyPrint)

  def map2D(d1:Int,d2:Int) : Int = 2*d1 + d2
  def map3D(d1:Int,d2:Int,d3:Int) : Int = 4*d1 + 2*d2 + d3

  val dirs2D : List[(Int,Int)] = List((0,0),(0,1),(1,0),(1,1))

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
