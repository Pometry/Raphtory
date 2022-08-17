package com.raphtory.algorithms.temporal.motif

import com.raphtory.api.analysis.algorithm.{Generic, GenericReduction}
import com.raphtory.api.analysis.graphview.{GraphPerspective, ReducedGraphPerspective}
import com.raphtory.api.analysis.table.{Row, Table}
import com.raphtory.api.analysis.visitor.{ExplodedEdge, ReducedVertex, Vertex}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}


class ThreeNodeMotifs(delta:Long=3600) extends GenericReduction {

  override def apply(graph: GraphPerspective): graph.ReducedGraph = {
    graph.reducedView.
      step {
        v=>
          val neighbours = v.neighbours.toSet
          neighbours.foreach(nb => v.messageVertex(nb,(v.ID, neighbours)))
      }
      // this step gets a list of (static) edges that form the opposite edge of a triangle with a node. this is ordered
      // smallest id first.
      .step { v =>
        val neighbours = v.neighbours.toSet
        val queue      = v.messageQueue[(v.IDType,Set[v.IDType])]
        val tri        = Set[(v.IDType,v.IDType)]()
        queue.foreach{
          // Pick a representative node who will hold all the exploded edge info.
          case (nb, nbSet) =>
            nbSet.intersect(neighbours).foreach{
              w =>
                // largest id node sends to the smallest id node the exploded edges.
                if (w.max(nb).max(v.ID)==v.ID) {
                  v.messageVertex(w.min(nb), v.explodedEdge(w.min(nb)).getOrElse(List()).map(e => (e.src,e.dst,e.timestamp)).sortBy(_._3))
                }
            }
        }
      }
      .step {
        v =>
          val opEdgeGroups = v.messageQueue[List[(Long,Long,Long)]]
          opEdgeGroups.foreach {
            group =>
              breakable {
                if (group.isEmpty){
                  break
                }
                val triMap : mutable.Map[(v.IDType, v.IDType),List[(Long,Long,Long)]] = mutable.Map()
                val (u,w,_) = group.head
                triMap.put((u.min(w),u.max(w)),group)
                triMap.put((v.ID.min(u),v.ID.max(u)), v.explodedEdge(u).getOrElse(List()).map(e => (e.src,e.dst,e.timestamp)).sortBy(_._3))
                triMap.put((v.ID.min(w),v.ID.max(w)), v.explodedEdge(w).getOrElse(List()).map(e => (e.src,e.dst,e.timestamp)).sortBy(_._3))
                val eMax = triMap.maxBy(x => (x._2.size, x._1._1.min(x._1._2)))
                val triMotifs = new TriadMotifCounter(eMax._1._1,eMax._1._2)
            }
          }
      }
      .step({
        v =>
          val mc = new StarMotifCounter(v.ID)
          mc.execute(v.explodeAllEdges().map(e=> (e.src,e.dst,e.timestamp)).sortBy(_._3),delta)
          val counts : Array[Int] = mc.getCounts.toArray
          v.neighbours.foreach{
            vid =>
              val mc2node = new TwoNodeMotifs(v.ID)
              mc2node.execute(v.explodedEdge(vid).getOrElse(List()).map(e => (e.src,e.dst,e.timestamp)).sortBy(_._3),delta)
              val twoNodeCounts = mc2node.getCounts
              for (i <- counts.indices) {
                counts.update(i, counts(i) - twoNodeCounts(i % 8))
              }
          }
          v.setState("counts",counts)
    })
  }

  override def tabularise(graph: ReducedGraphPerspective): Table = {
    graph.select(vertex => Row(vertex.name, vertex.getState[Array[Int]]("counts").toList))
  }

}

abstract class MotifCounter {

  val incoming: Int = 1
  val outgoing: Int = 0

  val dirsToMap = List((0,0),(0,1),(1,0),(1,1)).map { k=> (k,0)}
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

  val incoming: Int = 1
  val outgoing: Int = 0
  val uorv: Map[Long,Int] = Map(uid -> 0, vid -> 1)

  val dirsToMap = List((0,0,0),(0,0,1),(0,1,0),(0,1,1),(1,0,0),(1,0,1),(1,1,0),(1,1,1)).map { k=> (k,0)}
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
    curSum((1-isUorV,incoming,dir))+=curNodes((1-isUorV,incoming,nb))
    curSum((1-isUorV,outgoing,dir))+=curNodes((1-isUorV,outgoing,nb))
    curNodes(isUorV,dir,nb)+=1
  }

  def pop(curNodes: mutable.Map[(Int, Int, Long), Int], curSum: mutable.Map[(Int, Int, Int), Int], curEdge: (Long, Long, Long)): Unit = {
    if ((curEdge._1.min(curEdge._2),curEdge._1.max(curEdge._2))==(uid,vid))
      return
    val (isUorV,nb, dir) = if (curEdge._2 == uid || curEdge._2 ==vid) (uorv(curEdge._2),curEdge._1,incoming) else (uorv(curEdge._1),curEdge._2,outgoing)
    curNodes(isUorV,dir,nb)-=1
    curSum((isUorV,dir,incoming))+=curNodes((1-isUorV,incoming,nb))
    curSum((isUorV,dir,outgoing))+=curNodes((1-isUorV,outgoing,nb))
  }
  def processCurrent(curEdge: (Long, Long, Long)): Unit = {
    if ((curEdge._1.min(curEdge._2),curEdge._1.max(curEdge._2))!=(uid,vid)) {
      val (isUorV, nb, dir) = if (curEdge._2 == uid || curEdge._2 == vid) (uorv(curEdge._2), curEdge._1, incoming) else (uorv(curEdge._1), curEdge._2, outgoing)
      midSum((1-isUorV,incoming,dir))+=preNodes((1-isUorV,incoming,nb))
      midSum((1-isUorV,outgoing,dir))+=preNodes((1-isUorV,outgoing,nb))
      midSum((isUorV,dir,incoming))+=postNodes((1-isUorV,incoming,nb))
      midSum((isUorV,dir,outgoing))+=postNodes((1-isUorV,outgoing,nb))
    } else {
      val (utov,dir) = if (curEdge._1==uid) (1,outgoing) else (0,incoming)
//      finalCounts()
    }
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

  def getCounts: List[Int] = {
    (countPre.values++countPost.values++countMid.values).toList
  }

  }


class TwoNodeMotifs(vid: Long) {

  val incoming: Int = 1
  val outgoing: Int = -1

  val count1d: mutable.Map[Int,Int] = mutable.Map(-1 -> 0, 1 -> 0)
  val count2d: mutable.Map[(Int,Int),Int] = mutable.Map() ++= List((-1,-1),(-1,1),(1,-1),(1,1)).map { k=> (k,0)}
  val count3d: mutable.Map[(Int,Int,Int),Int] = mutable.Map() ++= List((-1,-1,-1),(-1,-1,1),(-1,1,-1),(-1,1,1),(1,-1,-1),(1,-1,1),(1,1,-1),(1,1,1)) map { k=> (k,0)}

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

  def getCounts:List[Int] = count3d.values.toList
}

object ThreeNodeMotifs {
  def apply(delta:Long=3600) = new ThreeNodeMotifs(delta)
}