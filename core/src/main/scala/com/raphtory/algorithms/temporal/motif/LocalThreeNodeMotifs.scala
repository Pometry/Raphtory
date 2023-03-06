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

import scala.collection.mutable.ArrayBuffer

/**
  *  {s}`LocalThreeNodeMotifs(delta:Long, graphWide:Boolean=true, prettyPrint:Boolean=true)`
  *    : Count occurrences of three-edge up-to-three-node temporal motifs each node participates in. For an efficient global count, use ThreeNodeMotifs
  *
  *  The algorithm is very based on that in "Motifs in Temporal Networks". An option is given to return results as a hashmap with labels or as an array which is easier for post-processing.
  *
  *  ## Parameters
  *  {s}`delta: Long=3600L`
  *   : Delta value for the maximum time length between the first and final edge of the motif. The default value is 3600L (1hour) assuming the data's timestamps are in seconds.
  *  {s}`prettyPrint: Boolean=True`
  *   : if `True`, returns a list of motifs with a description of the motif. Not recommended if you are doing any post processing on the outputted motifs.
  *  {s}`graphWide: Boolean=True`
  *  : if `True`, just one row per perspective is returned with counts for the whole graph, otherwise counts per vertex are returned. Note that the graph-wide counts are not just
  *  the sum of the per-vertex counts; the triangle counts are divided by three since they are counted once for each vertex.
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
  *   The motif counts are returned as a 40-d array where the first 24 elements are star counts,
  *   the next 8 are two-node motif counts and the final 8 are triangle counts.
  *
  * ## States
  *  {s}`starCounts: Array[Int]`
  *    : Three-node star motif counts stored as an array (see indices above)
  *  {s}`twoNodeCounts: Array[Int]`
  *    : Two-node motif counts stored as an array (see indices above)
  *  {s}`triCounts: Array[Int]`
  *    : Triangle motif counts stored as an array (see indices above)
  *
  * ## Returns
  *
  *  | vertex name       | motifs                   |
  *  | ----------------- | ------------------------ |
  *  | {s}`name: String` | {s}`motifCounts: Array[Long]` |
  */

class LocalThreeNodeMotifs(delta: Long = 3600, graphWide: Boolean = false, prettyPrint: Boolean = true)
        extends GenericReduction {
val outputList: List[String] = List("name") ++ (1 to 40).map(_.toString)
  val globalOutputList: Seq[String] = (1 to 40).map(_.toString)
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
        state.newAccumulator[Array[Long]](
                "twoNodeCounts",
                Array.fill(8)(0L),
                retainState = true,
                (ar1, ar2) => ar1.zip(ar2).map { case (x, y) => x + y }
        )
        state.newAccumulator[Array[Long]](
                "triCounts",
                Array.fill(8)(0L),
                retainState = true,
                (ar1, ar2) => ar1.zip(ar2).map { case (x, y) => x + y }
        )
        state.newAccumulator[Array[Long]](
                "starCounts",
                Array.fill(24)(0L),
                retainState = true,
                (ar1, ar2) => ar1.zip(ar2).map { case (x, y) => x + y }
        )
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
        v.setState("triCounts", Array.fill(8)(0L))
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
            mc.execute(inputEdges, delta)
            val curVal     = v.getState[Array[Long]]("triCounts")
            v.setState(
                    "triCounts",
                    curVal
                      .zip(mc.getCounts)
                      .map { case (x, y) => x + y }
            )
            v.messageVertex(u, mc.getCounts)
            v.messageVertex(w, mc.getCounts)
            state("triCounts") += mc.getCounts
          }
      }
      .step { v =>
        v.messageQueue[Array[Long]].foreach { toAdd =>
          val curVal = v.getState[Array[Long]]("triCounts")
          v.setState("triCounts", curVal.zip(toAdd).map { case (x, y) => x + y })
        }
      }
      .step { (v, state) =>
        val mc                  = new StarMotifCounter(v.ID, v.neighbours.filter(_ != v.ID))
        // Here we sort the edges not only by a timestamp but an additional index meaning that we obtain consistent results
        // for motif edges with the same timestamp
        mc.execute(v.explodeAllEdges().map(e => (e.src,e.dst,e.timestamp)).collect(selfLoopFilt).sortBy(x => (x._3, x._1, x._2)), delta)
        val counts: Array[Long] = mc.getCounts
        var twoNodeCounts       = Array.fill(8)(0L)
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
          val twoNC   = mc2node.getCounts
          for (i <- counts.indices)
            counts.update(i, counts(i) - twoNC(i % 8))
          twoNodeCounts = twoNodeCounts.zip(twoNC).map { case (x, y) => x + y }
        }
        v.setState("twoNodeCounts", twoNodeCounts)
        state("twoNodeCounts") += twoNodeCounts
        v.setState("starCounts", counts)
        state("starCounts") += counts

      }
  }

  override def tabularise(graph: ReducedGraphPerspective): Table =
    if (!graphWide)
      if (prettyPrint)
        graph
          .step { v =>
            v.setState("name", v.name())
            v.setState("starCounts", getStarCountsPretty(v.getState[Array[Long]]("starCounts")))
            v.setState("twoNodeCounts", get2NodeCountsWithoutRepeats(v.getState[Array[Long]]("twoNodeCounts")))
            v.setState("triCounts", getTriCountsPretty(v.getState[Array[Long]]("triCounts")))
          }
          .select("name", "starCounts", "twoNodeCounts", "triCounts")
      else
        graph
          .step { v =>
            v.setState("name", v.name())
            val motif_array = v.getState[Array[Long]]("starCounts") ++ v.getState[Array[Long]]("twoNodeCounts") ++ v
              .getState[Array[Long]]("triCounts")
            for (i <- 1 to 40) {
              v.setState(i.toString, motif_array(i-1))
            }
          }
          .select(outputList:_*)
    else if (prettyPrint)
      graph
        .setGlobalState { state =>
          state.newConstant[Map[String, Long]](
                  "starCountsPretty",
                  getStarCountsPretty(state[Array[Long], Array[Long]]("starCounts").value)
          )
          state.newConstant[Map[String, Int]](
                  "twoNodeCountsPretty",
                  get2NodeCountsWithoutRepeats(state[Array[Int], Array[Int]]("starCounts").value)
          )
          state.newConstant[Map[String, Int]](
                  "triCountsPretty",
                  getTriCountsPretty(state[Array[Int], Array[Int]]("starCounts").value)
          )
        }
        .globalSelect("starCountsPretty", "twoNodeCountsPretty", "triCountsPretty")
    else
      graph
        .setGlobalState { state =>
          val motif_array = state[Array[Long], Array[Long]]("starCounts").value ++ state[Array[Long], Array[Long]](
            "twoNodeCounts"
          ).value ++ state[Array[Long], Array[Long]]("triCounts").value
          for (i <- 1 to 40) {
            state.newConstant[Long](i.toString, motif_array(i-1))
          }
        }
        .globalSelect(globalOutputList:_*)
}

object LocalThreeNodeMotifs {

  def apply(delta: Long = 3600, graphWide: Boolean = false, prettyPrint: Boolean = true) =
    new LocalThreeNodeMotifs(delta, graphWide, prettyPrint)
}
