package com.raphtory.core.analysis.Algorithms

import java.time.LocalDateTime

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.API.entityVisitors.VertexVisitor

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable
import scala.collection.parallel.mutable.{ParArray, ParTrieMap}

class MultiLayerLPA(args:Array[String]) extends LPA(args) {
  val snapshotSize:Long = args(2).toLong
  val startTime = args(0).toLong * snapshotSize //TODO: change this when done with wsdata
  val endTime = args(1).toLong * snapshotSize
  val snapshots = (startTime to endTime by snapshotSize).tail
  println("Start: Multi layer LPA analysis - ", LocalDateTime.now())
  override def setup(): Unit = {
    view.getVertices().foreach { vertex =>
      val tlabels = snapshots.filter(t=> vertex.aliveAtWithWindow(t, snapshotSize))
        .toArray.map{t=>(t,//(t.toString+vertex.ID().toString).toLong)}
        scala.util.Random.nextLong())}
      vertex.setState("lpalabel", tlabels) // ArrayBuffer[(ts, lab)]
      vertex.messageAllNeighbours((vertex.ID(), tlabels)) //ArrayBuffer[(ID, ArrayBuffer[(ts, lab)])]
    }
  }

  override def analyse(): Unit = {
    try{
    view.getMessagedVertices().foreach { vertex =>
      val vlabel = vertex.getOrSetState[Array[(Long, Long)]]("lpalabel", snapshots.filter(t=> vertex.aliveAtWithWindow(t, snapshotSize))
        .toArray.map{t=>(t,//(t.toString+vertex.ID().toString).toLong)}
        scala.util.Random.nextLong())})
      val msgq = vertex.messageQueue[(Long, Array[(Long, Long)])]
      var count = 0
      val newLabel = vlabel.map { tv =>
        val ts = tv._1
        val nei_ts = (vertex.getInCEdgesBetween(ts-snapshotSize, ts) ++ vertex.getOutEdgesBetween(ts-snapshotSize, ts)).map(_.ID()).toSet
        val nei = msgq.filter(x => nei_ts.contains(x._1)).flatMap(_._2).groupBy(_._1)(ts).map(_._2)
        nei.append(tv._2)
        //TODO: verify that this checks out
        nei.appendAll(vlabel.filter(x=>(x._1==ts+snapshotSize)|(x._1==ts-snapshotSize)).map(_._2))
        val newlab = labelProbability(nei.groupBy(identity))
        if (newlab == tv._2) {count += 1}
        (ts, newlab)
      }
      vertex.setState("lpalabel", newLabel) 
      vertex.messageAllNeighbours((vertex.ID(), newLabel))
      if (count == vlabel.length) {vertex.voteToHalt()}
    }
    }catch {
      case e: Exception => println(e)
    }
  }

  override def labelProbability(gp: Map[Long, ArrayBuffer[Long]]): Long ={
    //    label probability function to get rid of oscillation phenomena of synchronous LPA from [1]
    //    [1] https://doi.org/10.1016/j.neucom.2014.04.084

    val f = gp.map{lab =>  Math.pow(lab._2.size, 2)}//TODO: implement full method from paper
    val p = f.toArray.map(i => i/f.toArray.sum)
    val gpp = gp.keys zip p
    gpp.filter(x=>x._2 == gpp.maxBy(_._2)._2).maxBy(_._1)._1
  }

  override def returnResults(): Any =
    view.getVertices()
      .map(vertex => (vertex.getOrSetState[Array[(Long, Long)]]("lpalabel", Array(0, -1L)), vertex.getPropertyValue("Word").getOrElse("Unknown")))
      .flatMap(f => f._1.map { x => (x._2, f._2.toString + '_' + x._1.toString) })

  override def extractData(results:ArrayBuffer[Any]):fd ={
    val endResults = results.asInstanceOf[ArrayBuffer[ArrayBuffer[(Long, String)]]].flatten
    try {
      val grouped = endResults.groupBy(f => f._1).mapValues(f=>f.map(_._2))
      val groupedNonIslands = grouped.filter(x => x._2.size > 1)
      val biggest = grouped.maxBy(_._2.size)._2.size
      val sorted = groupedNonIslands.toArray.sortBy(_._2.size)(sortOrdering)
      val top5 = sorted.map(x => x._2.size).take(10)
      val total = grouped.size
      val totalWithoutIslands = groupedNonIslands.size
      val totalIslands = total - totalWithoutIslands
      val proportion = biggest.toFloat / grouped.map(x => x._2.size).sum
      val communities = sorted.map(x=>x._2).take(if(top_c==0) sorted.length else top_c)
      fd(top5, total, totalIslands, proportion, communities)
    } catch {
      case e: UnsupportedOperationException => fd(Array(0),0,0,0,Array(ArrayBuffer("0")))
    }
  }

  override def defineMaxSteps(): Int = 100

}
