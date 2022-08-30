package com.raphtory.algorithms.temporal.motif

import com.raphtory.api.analysis.algorithm.{Generic, GenericReduction}
import com.raphtory.api.analysis.graphview.{GraphPerspective, ReducedGraphPerspective}
import com.raphtory.api.analysis.table.{Row, Table}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  *  {s}`ThreeNodeMotifs(delta:Long, graphWide:Boolean=true, prettyPrint:Boolean=true)`
  *    : Count occurrences of three-edge up-to-three-node temporal motifs in the network. The parmer
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

class LocalThreeNodeMotifs(delta:Long=3600, graphWide:Boolean=false, prettyPrint:Boolean=true) extends GenericReduction {

  override def apply(graph: GraphPerspective): graph.ReducedGraph = {
    graph.reducedView
      .setGlobalState{
        state =>
          state.newAccumulator[Array[Long]]("twoNodeCounts",Array.fill(8)(0L), retainState = true, (ar1, ar2) => ar1.zip(ar2).map{case (x,y) => x + y})
          state.newAccumulator[Array[Long]]("triCounts",Array.fill(8)(0L), retainState = true, (ar1, ar2) => ar1.zip(ar2).map{case (x,y) => x + y})
          state.newAccumulator[Array[Long]]("starCounts",Array.fill(24)(0L), retainState = true, (ar1, ar2) => ar1.zip(ar2).map{case (x,y) => x + y})
      }
      .step {
        v=>
          val neighbours = v.neighbours.filter(_!=v.ID).toSet
          neighbours.foreach{nb =>
            v.messageVertex(nb,(v.ID, neighbours))
          }
          v.setState("triCounts",Array.fill(8)(0))
      }
      .step { v =>
        val neighbours = v.neighbours.filter(_!=v.ID).toSet
        val queue      = v.messageQueue[(v.IDType,Set[v.IDType])]
        queue.foreach{
          // Pick a representative node who will hold all the exploded edge info.
          case (nb, friendsOfFriend) =>
            if (v.ID < nb) {
            friendsOfFriend.intersect(neighbours).foreach{
              w =>
                // largest id node sends to the smallest id node the exploded edges.
                  // message the edge history
                  v.messageVertex(w,v.explodedEdge(nb).getOrElse(List()).map(e=> (e.src, e.dst, e.timestamp)))
                }
            }
        }
      }
      .step{
        (v, state) =>
          v.messageQueue[List[(Long,Long,Long)]].foreach{
            edges =>
              val (u, w, _) = edges.head
              val inputEdges = (v.explodedEdge(u).getOrElse(List()).map(e => (e.src, e.dst, e.timestamp)) ++
                v.explodedEdge(w).getOrElse(List()).map(e => (e.src, e.dst, e.timestamp)) ++
                edges)
                .sortBy(x => (x._3, x._1, x._2))
              val ids = List(v.ID,u,w).sortWith(_<_)
              val mc = new TriadMotifCounter(ids(0),ids(1))
              mc.execute(inputEdges,delta)
              val curVal = v.getState[Array[Int]]("triCounts")
              v.setState("triCounts", curVal.zip(mc.getCounts)
                .map{ case(x,y)=> x+y})
              state("triCounts")+=mc.getCounts.map(_.toLong)
          }
      }
      .step({
        (v, state) =>
          val mc = new StarMotifCounter(v.ID)
          mc.execute(v.explodeAllEdges().map(e=> (e.src,e.dst,e.timestamp)).sortBy(x => (x._3, x._1, x._2)),delta)
          val counts : Array[Int] = mc.getCounts
          var twoNodeCounts = Array.fill(8)(0)
          v.neighbours.foreach{
            vid =>
              val mc2node = new TwoNodeMotifs(v.ID)
              mc2node.execute(v.explodedEdge(vid).getOrElse(List()).map(e => (e.src,e.dst,e.timestamp)).sortBy(x => (x._3, x._1, x._2)),delta)
              val twoNC = mc2node.getCounts
              for (i <- counts.indices) {
                counts.update(i, counts(i) - twoNC(i % 8))
              }
              twoNodeCounts = twoNodeCounts.zip(twoNC).map{ case (x,y) => x+y}
          }
          v.setState("twoNodeCounts",twoNodeCounts)
          state("twoNodeCounts")+=twoNodeCounts.map(_.toLong)
          v.setState("starCounts",counts)
          state("starCounts")+=counts.map(_.toLong)
      })
  }

  override def tabularise(graph: ReducedGraphPerspective): Table = {
    if (!graphWide) {
      if (prettyPrint)
        graph.select(vertex => Row(vertex.name, getStarCountsPretty(vertex.getState[Array[Int]]("starCounts")), get2NodeCountsWithoutRepeats(vertex.getState[Array[Int]]("twoNodeCounts")), getTriCountsPretty(vertex.getState[Array[Int]]("triCounts"))))
      else
        graph.select(vertex => Row(vertex.name, (vertex.getState[Array[Int]]("starCounts")++vertex.getState[Array[Int]]("twoNodeCounts")++vertex.getState[Array[Int]]("triCounts")).mkString("(", ";", ")")))
    } else {
      if (prettyPrint)
        graph.globalSelect(state => Row(getStarCountsPretty(state[Array[Long],Array[Long]]("starCounts").value), get2NodeCountsWithoutRepeats(state[Array[Int],Array[Int]]("twoNodeCounts").value), getTriCountsPretty(state[Array[Int],Array[Int]]("triCounts").value)))
      else
        graph.globalSelect(state => Row((state[Array[Long],Array[Long]]("starCounts").value++state[Array[Int],Array[Int]]("twoNodeCounts").value++state[Array[Int],Array[Int]]("triCounts").value).mkString("(", ";", ")")))
    }
  }

  def getTriCountsPretty[T:Numeric](triCounts:Array[T]): Map[String,T] = {
    val prettyCounts = mutable.Map[String,T]()
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

  def get2NodeCountsPretty[T:Numeric](counts:Array[T]): Map[String,T] = {
    val prettyCounts = mutable.Map[String,T]()
    prettyCounts.put("III",counts(0))
    prettyCounts.put("IIO",counts(1))
    prettyCounts.put("IOI",counts(2))
    prettyCounts.put("IOO",counts(3))
    prettyCounts.put("OII",counts(4))
    prettyCounts.put("OIO",counts(5))
    prettyCounts.put("OOI",counts(6))
    prettyCounts.put("OOO",counts(7))
    prettyCounts.toMap
  }

  def get2NodeCountsWithoutRepeats[T:Numeric](counts:Array[T]): Map[String,T] = {
    get2NodeCountsPretty(counts).map{case (k,v) => ("2NODE-"+k,v)}
  }

  def getStarCountsPretty[T:Numeric](counts:Array[T]): Map[String,T] = {
    val preMap = get2NodeCountsPretty(counts.slice(0,8)).map{case (k,v) => ("STAR-PRE-"+k,v)}
    val midMap = get2NodeCountsPretty(counts.slice(8,16)).map{case (k,v) => ("STAR-MID-"+k,v)}
    val postMap = get2NodeCountsPretty(counts.slice(16,24)).map{case (k,v) => ("STAR-POST-"+k,v)}
    preMap++midMap++postMap
  }

}

object LocalThreeNodeMotifs {
  def apply(delta:Long=3600, graphWide:Boolean=false, prettyPrint:Boolean=true) = new LocalThreeNodeMotifs(delta,graphWide,prettyPrint)
}