package com.raphtory.algorithms.temporal.motif

import com.raphtory.api.analysis.algorithm.{Generic, GenericReduction}
import com.raphtory.api.analysis.graphview.{GraphPerspective, ReducedGraphPerspective}
import com.raphtory.api.analysis.table.{Row, Table}
import com.raphtory.api.analysis.visitor.{ExplodedEdge, ReducedVertex, Vertex}

import scala.collection.mutable


class ThreeNodeMotifs(delta:Long=3600) extends GenericReduction {

  override def apply(graph: GraphPerspective): graph.ReducedGraph = {
    graph.reducedView.
      step {
        v=>
          val neighbours = v.neighbours.toSet
          neighbours.foreach{nb =>
              v.messageVertex(nb,(v.ID, neighbours))
          }
          v.setState("triCounts",Array.fill(8)(0))
      }
      // this step gets a list of (static) edges that form the opposite edge of a triangle with a node. this is ordered
      // smallest id first.
      .step { v =>
        val neighbours = v.neighbours.toSet
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
        v=>
          v.neighbours.filter(_>v.ID).foreach{
            nb =>
              val edge = v.getEdge(nb).minBy(_.src)
              val a_e = edge.getStateOrElse[Array[(Long,Long,Long)]]("a_e",Array())
              if (a_e.nonEmpty) {
                val mc = new TriadMotifCounter(v.ID, nb)
                val inputEdges = a_e
                  .appendedAll(v.explodedEdge(nb).getOrElse(List())
                  .map(e=> (e.src,e.dst,e.timestamp)))
                  .toList
                  .sortBy(_._3)
                mc.execute(inputEdges,delta)
                val triadCounts = v.getState[Array[Int]]("triCounts")
                v.setState("triCounts", triadCounts.zip(mc.getCounts).map{ case(x,y)=> x+y})
              }
          }
      }
      .step({
        v =>
          val mc = new StarMotifCounter(v.ID)
          mc.execute(v.explodeAllEdges().map(e=> (e.src,e.dst,e.timestamp)).sortBy(_._3),delta)
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
          v.setState("starCounts",counts)
    })
  }

  override def tabularise(graph: ReducedGraphPerspective): Table = {
    graph.select(vertex => Row(vertex.name, vertex.getState[Array[Int]]("starCounts").toList, vertex.getState[Array[Int]]("twoNodeCounts").toList, vertex.getState[Array[Int]]("triCounts").toList))
  }

}

abstract class MotifCounter {

  val incoming: Int = 0
  val outgoing: Int = 1

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
    finalCounts.values.toArray
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
    (countPre.values++countPost.values++countMid.values).toArray
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

  def getCounts:Array[Int] = count3d.values.toArray
}

case class RequestEdges(sendTo:Long, dst:Long)

object ThreeNodeMotifs {
  def apply(delta:Long=3600) = new ThreeNodeMotifs(delta)
}