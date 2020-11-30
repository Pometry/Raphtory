package com.raphtory.algorithms

import scala.collection.mutable.ArrayBuffer

class MultiLayerLPA(args:Array[String]) extends LPA(args) {
  val snapshotSize:Long = args(2).toLong
  override def setup(): Unit = {
    val startTime = args(0).toLong
    val endTime = args(1).toLong
    val snapshots = startTime to endTime by snapshotSize
    view.getVertices().foreach { vertex =>
      val tlabels = snapshots.filter(t=> vertex.aliveAtWithWindow(t, snapshotSize))
        .toArray.map{t=>(t,t+vertex.ID())}
      vertex.setState("lpalabel", tlabels) // ArrayBuffer[(ts, lab)]
      vertex.messageAllNeighbours((vertex.ID(), tlabels)) //ArrayBuffer[(ID, ArrayBuffer[(ts, lab)])]
    }
  }

  override def analyse(): Unit = {
    view.getMessagedVertices().foreach { vertex =>
      val vlabel = vertex.getState[Array[(Long, Long)]]("lpalabel")
      val msgq = vertex.messageQueue[(Long, Array[(Long, Long)])]
      var count = 0
      val newLabel = vlabel.map { tv =>
        val ts = tv._1
        val nei_ts = (vertex.getInCEdgesBetween(ts-snapshotSize, ts) ++ vertex.getOutEdgesBetween(ts-snapshotSize, ts)).map(_.ID()).toSet
        val nei = msgq.filter(x => nei_ts.contains(x._1)).flatMap(_._2).groupBy(_._1)(ts).map(_._2)
        nei.append(tv._2)
        //TODO: verify that this checks out
        nei.appendAll(vlabel.filter(x=>(x._1==ts+snapshotSize)&(x._1==ts-snapshotSize)).map(_._2))
        val newlab = labelProbability(nei.groupBy(identity))
        if (newlab == tv._2) {count += 1}
        (ts, newlab)
      }
      vertex.setState("lpalabel", newLabel) 
      vertex.messageAllNeighbours((vertex.ID(), newLabel))
      if (count == vlabel.length) {vertex.voteToHalt()}
    }
  }

  override def returnResults(): Any =
    view.getVertices()
      .map(vertex => (vertex.getState[Array[(Long, Long)]]("lpalabel"), vertex.ID()))
      .flatMap(f => f._1.map { x => (x._2, f._2.toString + '_' + x._1.toString) })

  override def extractData(results:ArrayBuffer[Any]):fd ={
    val endResults = results.asInstanceOf[ArrayBuffer[ArrayBuffer[(Long, String)]]].flatten
    try {
      val grouped = endResults.groupBy(f => f._1).mapValues(f=>f.map(_._2))
      val groupedNonIslands = grouped.filter(x => x._2.size > 1)
      val biggest = grouped.maxBy(_._2.size)._2.size
      val sorted = groupedNonIslands.toArray.sortBy(_._2.size)(sortOrdering).map(x => x._2.size)
      val top5 = if (sorted.length <= 10) sorted else sorted.take(10)
      val communities = grouped.values.toArray
      val total = grouped.size
      val totalWithoutIslands = groupedNonIslands.size
      val totalIslands = total - totalWithoutIslands
      val proportion = biggest.toFloat / grouped.map(x => x._2.size).sum
      val totalGT2 = grouped.count(x => x._2.size > 2)
      fd(top5, total, totalIslands, proportion, communities)
    } catch {
      case e: UnsupportedOperationException => fd(Array(0),0,0,0,Array(ArrayBuffer("0")))
    }
  }

  override def defineMaxSteps(): Int = 100

}
