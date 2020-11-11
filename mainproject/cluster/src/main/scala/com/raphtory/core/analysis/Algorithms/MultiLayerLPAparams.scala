package com.raphtory.core.analysis.Algorithms

import com.raphtory.core.analysis.API.entityVisitors.VertexVisitor

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ParMap

class MultiLayerLPAparams(args:Array[String]) extends LPA(args) {
  //args = [top_c, start, end, layer-size, w, theta, property]
  val snapshotSize:Long = args(3).toLong
  val startTime = args(1).toLong * snapshotSize //TODO: change this when done with wsdata
  val endTime = args(2).toLong * snapshotSize
  val snapshots = (startTime to endTime by snapshotSize).tail
  val weight = args(4)
  val theta = args(5).toDouble
  override val PROP = args(6)

  override def setup(): Unit = {
    view.getVertices().foreach { vertex =>
      val tlabels = snapshots.filter(t=> vertex.aliveAtWithWindow(t, snapshotSize))
        .toArray.map{t=>(t,//(t.toString+vertex.ID().toString).toLong)}
        List(-1L, scala.util.Random.nextLong()))}
      vertex.setState("lpalabel", tlabels) // ArrayBuffer[(ts, lab)]
      vertex.messageAllNeighbours((vertex.ID(), tlabels)) //ArrayBuffer[(ID, ArrayBuffer[(ts, lab)])]
    }
  }


  override def analyse(): Unit = {
    try{
    view.getMessagedVertices().foreach { vertex =>
      val vlabel = vertex.getOrSetState[Array[(Long, List[Long])]]("lpalabel", snapshots.filter(t=> vertex.aliveAtWithWindow(t, snapshotSize))
        .toArray.map{t=>(t,//(t.toString+vertex.ID().toString).toLong)}
        List(-1L,scala.util.Random.nextLong()))})
      val msgq = vertex.messageQueue[(Long, Array[(Long, List[Long])])]
      var count = 0
      val newLabel = vlabel.map { tv =>
        val ts = tv._1
        val nei_ts_freq = weightFunction(vertex, ts) // ID -> freq
        val nei_labs = msgq.filter(x => nei_ts_freq.keySet.contains(x._1)).map{ x=> (x._2.filter(_._1==ts).map(_._2.last).head, nei_ts_freq(x._1))} //(lab, freq)
        val v_tempo_nei_labs = vlabel.filter(_._1==ts+snapshotSize).map(x=> (x._2.last, interLayerWeights(weight, vertex, ts)))++
          vlabel.filter(_._1==ts-snapshotSize).map(x=>(x._2.last,interLayerWeights(weight, vertex, ts-snapshotSize)))
        nei_labs.appendAll(v_tempo_nei_labs)
        val max_freq = nei_labs.groupBy(_._1).mapValues(_.map(_._2).sum)
        var newlab = max_freq.filter(f=> f._2 == max_freq.values.max).keySet.max
        if (tv._2.contains(newlab)) {
          newlab = tv._2.max
          count += 1}
        (ts, tv._2.tail.union(List(newlab)))
      }
      vertex.setState("lpalabel", newLabel)
      vertex.messageAllNeighbours((vertex.ID(), newLabel))
      if (count == vlabel.length) {vertex.voteToHalt()}
    }
    }catch {
      case e: Exception => println(e)
    }
  }

  def interLayerWeights(x:String,v:VertexVisitor, ts:Long): Long = {
    x match {
      case "none" =>
        val neilabs = weightFunction(v, ts)
          neilabs.values.sum / neilabs.size
      case _ => weight.toLong
    }
  }

  def weightFunction(v:VertexVisitor, ts:Long): ParMap[Long, Long] ={
    val nei_ts = (v.getInCEdgesBetween(ts-snapshotSize, ts) ++ v.getOutEdgesBetween(ts-snapshotSize, ts))
      .filter(e => e.getPropertyValueAt("ScaledFreq", ts).getOrElse(1.0).asInstanceOf[Double] > theta)
    nei_ts.map { e => (e.ID(), e.getPropertyValueAt(PROP, ts).getOrElse(1L).asInstanceOf[Long]) }
      .groupBy(_._1).mapValues(x=> x.map(_._2).sum) // (ID -> Freq)
  }
  override def returnResults(): Any =
    view.getVertices()
      .map(vertex => (vertex.getOrSetState[Array[(Long, List[Long])]]("lpalabel", Array(0, -1L)), vertex.getPropertyValue("Word").getOrElse("Unknown")))
      .flatMap(f => f._1.map { x => (x._2.last, f._2.toString + '_' + x._1.toString) })

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
