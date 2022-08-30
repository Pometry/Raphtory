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

class ThreeNodeMotifs(delta:Long=3600, graphWide:Boolean=false, prettyPrint:Boolean=true) extends GenericReduction {

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
          // Ensure no self loops are counted within motifs
          val neighbours = v.neighbours.filter(_!=v.ID).toSet
          neighbours.foreach{nb =>
              v.messageVertex(nb,(v.ID, neighbours))
          }
          v.setState("triCounts",Array.fill(8)(0))
      }
      // this step gets a list of (static) edges that form the opposite edge of a triangle with a node. this is ordered
      // smallest id first.
      .step { v =>
        // Ensure no self loops are counted within motifs
        val neighbours = v.neighbours.filter(_!=v.ID).toSet
        val queue      = v.messageQueue[(v.IDType,Set[v.IDType])]
        queue.foreach{
          // Pick a representative node who will hold all the exploded edge info.
          case (nb, friendsOfFriend) =>
            friendsOfFriend.intersect(neighbours).foreach{
              w =>
                // largest id node sends to the smallest id node the exploded edges.
                if (nb < w && w.max(nb).max(v.ID)==v.ID) {
                  // message the edge and the size of the edge history
                  v.messageVertex(nb,(v.ID.min(w), v.ID.max(w), v.explodedEdge(w).getOrElse(List()).size))
                }
            }
        }
      }
      // in this step only the smallest id node in the triangle is looking after the process of finding emax.
      .step {
        v =>
          val opEdgeSizes = v.messageQueue[(Long,Long,Int)]
          opEdgeSizes.foreach {
            edge =>
                val triMap : mutable.Map[(Long, Long),Int] = mutable.Map()
                // already ordered in id size from u to w.
                val (u,w,size) = (edge._1, edge._2, edge._3)
                triMap.put((u,w),size)
                triMap.put((v.ID,u), v.explodedEdge(u).getOrElse(List()).size)
                triMap.put((v.ID,w), v.explodedEdge(w).getOrElse(List()).size)
                // order first by size then by id1 then id2 to ensure uniqueness.
                val eMax = triMap.maxBy(x => (x._2, x._1._1, x._1._2))._1
              if (eMax._1 == v.ID) {
                // request the history of the other edges
                v.messageVertex(eMax._2,RequestEdges(v.ID,if (eMax._2 == u) w else u))
                val appendedEdge = v.getEdge(eMax._2).minBy(_.src)
                v.explodedEdge(if (eMax._2 == u) w else u).getOrElse(List()).foreach(e=> appendedEdge.appendToState[(Long,Long,Long)]("a_e",(e.src,e.dst,e.timestamp)))
              } else {
                // send to the vertex looking after eMax
                v.messageVertex(u,(w, v.explodedEdge(w).getOrElse(List()).map(e => (e.src,e.dst,e.timestamp))))
              }
          }
      }
      // Either return edges to the requested vertex or set exploded edges received as edge state
      .step{
      v =>
        val queue = v.messageQueue[Any]
        queue.foreach{
          case RequestEdges(sendTo, dst) =>
            v.messageVertex(sendTo, (v.ID, v.explodedEdge(dst).getOrElse(List()).map(e => (e.src,e.dst,e.timestamp))))
          case edges:(Long,List[(Long,Long,Long)]) =>
            edges._2.foreach( e => v.getEdge(edges._1).minBy(_.src).appendToState[(Long,Long,Long)]("a_e",e))
            val dst = if (edges._2.head._1 == edges._1) edges._2.head._2 else edges._2.head._1
            val appendedEdge = v.getEdge(edges._1).minBy(_.src)
            v.explodedEdge(dst).getOrElse(List()).foreach(e => appendedEdge.appendToState[(Long,Long,Long)]("a_e",(e.src,e.dst,e.timestamp)))
        }
    }
      .step{
        v =>
          v.messageQueue[(Long,List[(Long,Long,Long)])].foreach{
            edges =>
              edges._2.foreach {
                e =>
                  v.getEdge(edges._1)
                  .minBy(_.src)
                  .appendToState[(Long,Long,Long)]("a_e",e)
              }
          }
      }
      .step{
        (v, state) =>
          v.neighbours.filter(_>v.ID).foreach{
            nb =>
              val edge = v.getEdge(nb).minBy(_.src)
              val a_e = edge.getStateOrElse[ArrayBuffer[(Long,Long,Long)]]("a_e",ArrayBuffer())
              if (a_e.nonEmpty) {
                val mc = new TriadMotifCounter(v.ID, nb)
                val inputEdges = a_e
                  .appendedAll(v.explodedEdge(nb).getOrElse(List())
                  .map(e=> (e.src,e.dst,e.timestamp)))
                  .toList
                  .sortBy(x => (x._3, x._1, x._2))
                mc.execute(inputEdges,delta)
                val curVal = v.getState[Array[Int]]("triCounts")
                v.setState("triCounts", curVal.zip(mc.getCounts)
                  .map{ case(x,y)=> x+y})
                state("triCounts")+=mc.getCounts.map(_.toLong)
              }
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
              mc2node.execute(v.explodedEdge(vid).getOrElse(List()).map(e => (e.src,e.dst,e.timestamp)).sortBy(_._3),delta)
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

abstract class MotifCounter {

  val incoming: Int = 0
  val outgoing: Int = 1

  val dirsToMap: Seq[((Int, Int), Int)] = List((0,0),(0,1),(1,0),(1,1)).map { k=> (k,0)}
  val preNodes: mutable.Map[(Int, Long), Int] = mutable.Map[(Int,Long),Int]().withDefaultValue(0)
  val postNodes: mutable.Map[(Int, Long), Int] = mutable.Map[(Int,Long),Int]().withDefaultValue(0)
  val preSum: mutable.Map[(Int, Int), Int] = mutable.Map[(Int,Int),Int]()++=dirsToMap
  val midSum: mutable.Map[(Int, Int), Int] = mutable.Map[(Int,Int),Int]()++=dirsToMap
  val postSum: mutable.Map[(Int, Int), Int] = mutable.Map[(Int,Int),Int]()++=dirsToMap

  def push(curNodes: mutable.Map[(Int, Long), Int], curSum: mutable.Map[(Int, Int), Int], curEdge: (Long, Long, Long)): Unit
  def pop(curNodes: mutable.Map[(Int, Long), Int], curSum:mutable.Map[(Int, Int), Int], curEdge:(Long,Long,Long)): Unit
  def processCurrent(curEdge:(Long,Long,Long)): Unit

  def execute(edges:List[(Long,Long,Long)], delta:Long): Unit = {
    val L = edges.size
    if (L<3)
      return
    var start = 0
    var end = 0
    for (j <- 0 until L) {
      while (start < L && edges(start)._3 + delta < edges(j)._3) {
        pop(preNodes,preSum,edges(start))
        start+=1
      }
      while(end < L && edges(end)._3 <= edges(j)._3 + delta) {
        push(postNodes,postSum,edges(end))
        end+=1
      }
      pop(postNodes,postSum,edges(j))
      processCurrent(edges(j))
      push(preNodes,preSum,edges(j))
    }
  }
}

class TriadMotifCounter(uid: Long, vid: Long) {

  val incoming: Int = 0
  val outgoing: Int = 1
  val uorv: Map[Long,Int] = Map(uid -> 0, vid -> 1)

  val dirsToMap: Seq[((Int, Int, Int), Int)] = List((0,0,0),(0,0,1),(0,1,0),(0,1,1),(1,0,0),(1,0,1),(1,1,0),(1,1,1)).map { k=> (k,0)}
  val preNodes: mutable.Map[(Int, Int, Long), Int] = mutable.Map[(Int,Int,Long),Int]().withDefaultValue(0)
  val postNodes: mutable.Map[(Int, Int, Long), Int] = mutable.Map[(Int,Int,Long),Int]().withDefaultValue(0)
  val preSum: mutable.Map[(Int, Int, Int), Int] = mutable.Map[(Int,Int,Int),Int]()++=dirsToMap
  val midSum: mutable.Map[(Int, Int, Int), Int] = mutable.Map[(Int,Int,Int),Int]()++=dirsToMap
  val postSum: mutable.Map[(Int, Int, Int), Int] = mutable.Map[(Int,Int,Int),Int]()++=dirsToMap
  val finalCounts: mutable.Map[(Int, Int, Int), Int] = mutable.Map[(Int,Int,Int),Int]()++=dirsToMap

  def push(curNodes: mutable.Map[(Int, Int, Long), Int], curSum: mutable.Map[(Int, Int, Int), Int], curEdge: (Long, Long, Long)): Unit = {
    if ((curEdge._1.min(curEdge._2),curEdge._1.max(curEdge._2))==(uid,vid))
      return
    val (isUorV,nb, dir) = if (curEdge._2 == uid || curEdge._2 ==vid) (uorv(curEdge._2),curEdge._1,incoming) else (uorv(curEdge._1),curEdge._2,outgoing)
    curSum((1-isUorV,incoming,dir))+=curNodes((incoming,1-isUorV,nb))
    curSum((1-isUorV,outgoing,dir))+=curNodes((outgoing,1-isUorV,nb))
    curNodes(dir,isUorV,nb)+=1
  }

  def execute(edges:List[(Long,Long,Long)], delta:Long): Unit = {
    val L = edges.size
    if (L<3)
      return
    var start = 0
    var end = 0
    for (j <- 0 until L) {
      while (start < L && edges(start)._3 + delta < edges(j)._3) {
        pop(preNodes,preSum,edges(start))
        start+=1
      }
      while(end < L && edges(end)._3 <= edges(j)._3 + delta) {
        push(postNodes,postSum,edges(end))
        end+=1
      }
      pop(postNodes,postSum,edges(j))
      processCurrent(edges(j))
      push(preNodes,preSum,edges(j))
    }
  }

  def pop(curNodes: mutable.Map[(Int, Int, Long), Int], curSum: mutable.Map[(Int, Int, Int), Int], curEdge: (Long, Long, Long)): Unit = {
    if ((curEdge._1.min(curEdge._2),curEdge._1.max(curEdge._2))==(uid,vid))
      return
    val (isUorV,nb, dir) = if (curEdge._2 == uid || curEdge._2 ==vid) (uorv(curEdge._2),curEdge._1,incoming) else (uorv(curEdge._1),curEdge._2,outgoing)
    curNodes(dir,isUorV,nb)-=1
    curSum((isUorV,dir,incoming))-=curNodes((incoming,1-isUorV,nb))
    curSum((isUorV,dir,outgoing))-=curNodes((outgoing,1-isUorV,nb))
  }

  def processCurrent(curEdge: (Long, Long, Long)): Unit = {
    if ((curEdge._1.min(curEdge._2),curEdge._1.max(curEdge._2))!=(uid,vid)) {
      val (isUorV, nb, dir) = if (curEdge._2 == uid || curEdge._2 == vid) (uorv(curEdge._2), curEdge._1, incoming) else (uorv(curEdge._1), curEdge._2, outgoing)
      midSum((1-isUorV,incoming,dir))-=preNodes((incoming,1-isUorV,nb))
      midSum((1-isUorV,outgoing,dir))-=preNodes((outgoing,1-isUorV,nb))
      midSum((isUorV,dir,incoming))+=postNodes((incoming,1-isUorV,nb))
      midSum((isUorV,dir,outgoing))+=postNodes((outgoing,1-isUorV,nb))
    } else {
      val (utov,dir) = if (curEdge._1==uid) (1,outgoing) else (0,incoming)
      finalCounts(0,0,0) += midSum(utov, 0,0) + postSum(utov,0,1) + preSum(1-utov,1,1)
      finalCounts(1,0,0) += midSum(utov,1,0) + postSum(1-utov,0,1) + preSum(1-utov,0,1)
      finalCounts(0,1,0) += midSum(1-utov,0,0) + postSum(utov,1,1) + preSum(1-utov,1,0)
      finalCounts(1,1,0) += midSum(1-utov,1,0) + postSum(1-utov,1,1) + preSum(1-utov,0,0)
      finalCounts(0,0,1) += midSum(utov,0,1) + postSum(utov,0,0) + preSum(utov,1,1)
      finalCounts(1,0,1) += midSum(utov,1,1) + postSum(1-utov,0,0) + preSum(utov,0,1)
      finalCounts(0,1,1) += midSum(1-utov,0,1) + postSum(utov,1,0) + preSum(utov,1,0)
      finalCounts(1,1,1) += midSum(1-utov,1,1) + postSum(1-utov,1,0) + preSum(utov,0,0)
    }
  }

  def getCounts: Array[Int] = {
    finalCounts.toSeq.sortBy(_._1).map(_._2).toArray
  }
}

class StarMotifCounter(vid: Long) extends MotifCounter {

  val defaultCounter = List((0,0,0),(0,0,1),(0,1,0),(0,1,1),(1,0,0),(1,0,1),(1,1,0),(1,1,1)) map { k=> (k,0)}
  val countPre: mutable.Map[(Int,Int,Int),Int] = mutable.Map[(Int,Int,Int),Int]() ++=defaultCounter
  val countMid: mutable.Map[(Int,Int,Int),Int] = mutable.Map[(Int,Int,Int),Int]() ++=defaultCounter
  val countPost: mutable.Map[(Int,Int,Int),Int] = mutable.Map[(Int,Int,Int),Int]() ++=defaultCounter

  override def push(curNodes: mutable.Map[(Int, Long), Int], curSum: mutable.Map[(Int, Int), Int], curEdge: (Long, Long, Long)): Unit = {
    val (nb, dir) = if (curEdge._2 == vid) (curEdge._1, incoming) else (curEdge._2, outgoing)
    curSum.mapValuesInPlace({
      case ((dir1,dir2),ct) =>
        if (dir2 == dir) ct + curNodes((dir1,nb)) else ct
    })
    curNodes((dir,nb))+=1
  }

  override def pop(curNodes: mutable.Map[(Int, Long), Int], curSum: mutable.Map[(Int, Int), Int], curEdge: (Long,Long,Long)): Unit = {
    val (nb, dir) = if (curEdge._2 == vid) (curEdge._1, incoming) else (curEdge._2, outgoing)
    curNodes((dir,nb))-=1
    curSum.mapValuesInPlace({
      case ((dir1,dir2),ct) =>
        if (dir1 == dir) ct - curNodes((dir2,nb)) else ct
    })
  }

  override def processCurrent(curEdge: (Long,Long,Long)): Unit = {
    val (nb, dir) = if (curEdge._2 == vid) (curEdge._1, incoming) else (curEdge._2, outgoing)
    midSum.mapValuesInPlace({
      case ((dir1, dir2), ct) =>
        if (dir2 == dir) ct - preNodes((dir1,nb)) else ct
    })
    countPre.mapValuesInPlace({
      case ((dir1,dir2,dir3),ct) =>
        if (dir3 == dir) ct + preSum((dir1,dir2)) else ct
    })
    countPost.mapValuesInPlace({
      case ((dir1,dir2,dir3),ct) =>
        if (dir1 == dir) ct + postSum((dir2,dir3)) else ct
    })
    countMid.mapValuesInPlace({
      case ((dir1,dir2,dir3),ct) =>
        if (dir2 == dir) ct + midSum((dir1,dir3)) else ct
    })
    midSum.mapValuesInPlace({
      case ((dir1, dir2), ct) =>
        if (dir1 == dir) ct + postNodes((dir2,nb)) else ct
    })
  }

  def getCounts: Array[Int] = {
    (countPre.toSeq.sortBy(_._1).map(_._2)++countMid.toSeq.sortBy(_._1).map(_._2)++countPost.toSeq.sortBy(_._1).map(_._2)).toArray
  }

  }


class TwoNodeMotifs(vid: Long) {

  val incoming: Int = 0
  val outgoing: Int = 1

  val count1d: mutable.Map[Int,Int] = mutable.Map(0 -> 0, 1 -> 0)
  val count2d: mutable.Map[(Int,Int),Int] = mutable.Map() ++= List((0,0),(0,1),(1,0),(1,1)).map { k=> (k,0)}
  val count3d: mutable.Map[(Int,Int,Int),Int] = mutable.Map() ++= List((0,0,0),(0,0,1),(0,1,0),(0,1,1),(1,0,0),(1,0,1),(1,1,0),(1,1,1)) map { k=> (k,0)}

  def execute(edges:List[(Long,Long,Long)], delta:Long): Unit = {
    var start = 0
    for (end <- edges.indices) {
      while (edges(start)._3 + delta < edges(end)._3) {
        decrementCounts(edges(start))
        start+=1
      }
      incrementCounts(edges(end))
    }
  }

  def decrementCounts(edge:(Long,Long,Long)): Unit = {
    val dir = if (edge._2 == vid) incoming else outgoing
    count1d(dir)-=1
    count2d.mapValuesInPlace({
      case ((dir1,dir2),ct) =>
        if (dir1==dir) ct - count1d(dir2) else ct
     })
  }

  def incrementCounts(edge:(Long,Long,Long)): Unit = {
    val dir = if (edge._2 == vid) incoming else outgoing
    count3d.mapValuesInPlace({
      case ((dir1,dir2,dir3),ct) =>
        if (dir3==dir) ct + count2d((dir1,dir2)) else ct
    })
    count2d.mapValuesInPlace({
      case ((dir1,dir2),ct) =>
        if (dir2==dir) ct + count1d(dir1) else ct
    })
    count1d(dir)+=1
  }

  def getCounts:Array[Int] = {
    count3d.toSeq.sortBy(_._1).map(_._2).toArray
  }
}

case class RequestEdges(sendTo:Long, dst:Long)

object ThreeNodeMotifs {
  def apply(delta:Long=3600, graphWide:Boolean=false, prettyPrint:Boolean=true) = new ThreeNodeMotifs(delta,graphWide,prettyPrint)
}