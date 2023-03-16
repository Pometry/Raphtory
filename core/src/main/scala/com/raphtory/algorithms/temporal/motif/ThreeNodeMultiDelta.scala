package com.raphtory.algorithms.temporal.motif

import com.raphtory.algorithms.generic.KCore
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.algorithm.GenericReduction
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.graphview.ReducedGraphPerspective
import com.raphtory.api.analysis.table.Table
import com.raphtory.algorithms.temporal.motif.ThreeNodeMotifs.get2NodeCountsWithoutRepeats
import com.raphtory.algorithms.temporal.motif.ThreeNodeMotifs.getStarCountsPretty
import com.raphtory.algorithms.temporal.motif.ThreeNodeMotifs.getTriCountsPretty
import com.raphtory.api.analysis.visitor.ExplodedEdge
import com.raphtory.internals.communication.SchemaProviderInstances._
import com.raphtory.sinks.FileSink

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  *  {s}`LocalThreeNodeMotifs(delta:Long, graphWide:Boolean=true, prettyPrint:Boolean=true)`
  *    : Count occurrences of three-edge up-to-three-node temporal motifs each node participates in. See the ThreeNodeMotifs class and Local ThreeNodeMotifs
  *    class within the same package for more details.
  *
  *  ## Parameters
  *  {s}`deltas: List[Long]`
  *    : The list of deltas (motif completion windows) to use
  *
  *  | delta             | 1 2 ... 40                   |
  *  | ----------------- | ------------------------ |
  *  | {s}`delta: Long`  | {s}`motifCount: Long` |
  */

class ThreeNodeMultiDelta(deltas: Array[Long])
  extends GenericReduction {
  val outputList: List[String] = List("name") ++ (1 to 40).map(_.toString)
  val globalOutputList: Seq[String] = List("delta") ++ (1 to 40).map(_.toString)
  val selfLoopFilt: PartialFunction[(Long,Long,Long),(Long,Long,Long)] = {
    case (src,dst,time) if src!=dst => (src,dst,time)
  }
  override def apply(graph: GraphPerspective): graph.ReducedGraph = {
    // Here we apply a trick to make sure the triangles are only counted for nodes in the two-core.
    KCore(2)
      .apply(graph)
      .clearMessages()
      .reducedView
      .setGlobalState { state =>
        deltas.foreach { delta =>
          state.newAccumulator[Array[Long]](
            "twoNodeCounts"+delta.toString,
            Array.fill(8)(0L),
            retainState = true,
            (ar1, ar2) => ar1.zip(ar2).map { case (x, y) => x + y }
          )
          state.newAccumulator[Array[Long]](
            "triCounts"+delta.toString,
            Array.fill(8)(0L),
            retainState = true,
            (ar1, ar2) => ar1.zip(ar2).map { case (x, y) => x + y }
          )
          state.newAccumulator[Array[Long]](
            "starCounts"+delta.toString,
            Array.fill(24)(0L),
            retainState = true,
            (ar1, ar2) => ar1.zip(ar2).map { case (x, y) => x + y }
          )
        }
      }
      // Filter step 1: tell neighbours you are in the filter
      .step { v =>
        if (v.getState[Int]("effectiveDegree") >= 2)
          v.messageAllNeighbours(v.ID)
      }
      // Filter step 2: send neigbours in filter to other neighbours in filter
      .step { v =>
        if (v.getState[Int]("effectiveDegree") >= 2) {
          val neighbours = v.messageQueue[v.IDType].toSet
          v.setState("effNeighbours", neighbours)
          neighbours.foreach(nb => v.messageVertex(nb, (v.ID, neighbours)))
        }
      }
      .step { v =>
        if (v.getState[Int]("effectiveDegree") >= 2) {
          val neighbours = v.getState[Set[v.IDType]]("effNeighbours")
          v.clearState("effNeighbours")
          val queue      = v.messageQueue[(v.IDType, Set[v.IDType])]
          queue.foreach {
            // Pick a representative node who will hold all the exploded edge info.
            case (nb, friendsOfFriend) =>
              // Here we want to make sure only one node is computing each triangle count as the operation is expensive, pick this
              // to be the smallest id vertex which receives the exploded edge of the opposite triangle.
              if (v.ID > nb)
                friendsOfFriend.intersect(neighbours).foreach { w =>
                  if (nb > w)
                    v.messageVertex(w, v.explodedEdge(nb).getOrElse(List()).map(e => (e.src, e.dst, e.timestamp)))
                  // largest id node sends to the smallest id node the exploded edges.
                }
          }
        }
      }
      .step { (v, state) =>
        if (v.getState[Int]("effectiveDegree") >= 2)
          v.messageQueue[List[(Long, Long, Long)]].foreach { edges =>
            val (u, w, _)  = edges.head
            // Here we sort the edges not only by a timestamp but an additional index meaning that we obtain consistent results
            // for motif edges with the same timestamp
            val inputEdges = (v.explodedEdge(u).getOrElse(List()).map(e => (e.src, e.dst, e.timestamp)) ++
              v.explodedEdge(w).getOrElse(List()).map(e => (e.src, e.dst, e.timestamp)) ++
              edges)
              .sortBy(x => (x._3, x._1, x._2))
            val ids        = List(v.ID, u, w).sortWith(_ < _)
            val mc         = new TriadMotifCounter(ids(0), ids(1), List(ids(2)))
            deltas.foreach{
              delta =>
                mc.execute(inputEdges, delta)
                state("triCounts"+delta.toString) += mc.getCounts
            }
            }
      }
      .step { (v, state) =>
        // Here we sort the edges not only by a timestamp but an additional index meaning that we obtain consistent results
        // for motif edges with the same timestamp
        deltas.foreach {
          delta =>

            val mc                  = new StarMotifCounter(v.ID, v.neighbours.filter(_ != v.ID))
          mc.execute(v.explodeAllEdges().map(e => (e.src, e.dst, e.timestamp)).collect(selfLoopFilt).sortBy(x => (x._3, x._1, x._2)), delta)
          val counts: Array[Long] = mc.getCounts
          var twoNodeCounts = Array.fill(8)(0L)
          v.neighbours.foreach { vid =>
            val mc2node = new TwoNodeMotifs(v.ID)
            // Here we sort the edges not only by a timestamp but an additional index meaning that we obtain consistent results
            // for motif edges with the same timestamp
            mc2node.execute(
              v.explodedEdge(vid)
                .getOrElse(List())
                .map(e => (e.src, e.dst, e.timestamp))
                .sortBy(x => (x._3, x._1, x._2))
                .toArray,
              delta
            )
            val twoNC = mc2node.getCounts
            for (i <- counts.indices)
              counts.update(i, counts(i) - twoNC(i % 8))
            twoNodeCounts = twoNodeCounts.zip(twoNC).map { case (x, y) => x + y }
          }
          state("twoNodeCounts"+delta.toString) += twoNodeCounts
          state("starCounts"+delta.toString) += counts
        }
      }
  }

  override def tabularise(graph: ReducedGraphPerspective): Table =
      graph
        .setGlobalState { state =>
          state.newConstant[List[Long]]("delta",deltas.toList)
          for (i <- 1 to 24) {
            val motifDelta = deltas.map(d => state[Array[Long], Array[Long]]("starCounts"+d.toString).value(i-1))
            state.newConstant[List[Long]](i.toString, motifDelta.toList)
          }
          for (i <- 25 to 32) {
            val motifDelta = deltas.map(d => state[Array[Long], Array[Long]]("twoNodeCounts"+d.toString).value(i-25))
            state.newConstant[List[Long]](i.toString, motifDelta.toList)
          }
          for (i <- 33 to 40) {
            val motifDelta = deltas.map(d => state[Array[Long], Array[Long]]("triCounts"+d.toString).value(i-33))
            state.newConstant[List[Long]](i.toString, motifDelta.toList)
          }
        }
        .globalSelect(globalOutputList:_*)
        .explode(globalOutputList:_*)
}

object ThreeNodeMultiDelta {

  def apply(deltas: Array[Long]) =
    new ThreeNodeMultiDelta(deltas)
}
