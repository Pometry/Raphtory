package com.raphtory.core.analysis.Algorithms

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.API.entityVisitors.VertexVisitor

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable
import scala.collection.parallel.mutable.{ParArray, ParTrieMap}

class MultiLayerLPA(args:Array[String]) extends LPA(args) {
  override def setup(): Unit = {
    view.getVertices().foreach { vertex =>
      val tlabels = vertex.getHistory().map { t =>
        if (t._2) { (t._1, t._1+vertex.ID())}
      }
      vertex.setState("lpalabel", tlabels) // ArrayBuffer[(ts, lab)]
      vertex.messageAllNeighbours((vertex.ID(), tlabels)) //ArrayBuffer[(ID, ArrayBuffer[(ts, lab)])]
    }
  }

  override def analyse(): Unit = {
    view.getMessagedVertices().foreach { vertex =>
      val vlabel = vertex.getState[ArrayBuffer[(Long, Long)]]("lpalabel")
      val msgq = vertex.messageQueue[(Long, ArrayBuffer[(Long, Long)])]
      var count = 0
      val newLabel = vlabel.map { tv =>
        val ts = tv._1
        val nei_ts = (vertex.getInCEdgesBetween(ts, ts) ++ vertex.getOutEdgesBetween(ts, ts)).map(_.ID()).toSet
        val nei = msgq.filter(x => nei_ts.contains(x._1)).flatMap(_._2).groupBy(_._1)(ts).map(_._2)
        nei.append(tv._2)
        //TODO: find better way to do this
        val uppertn = vlabel.filter(x => x._1 > ts)
        if (uppertn.nonEmpty) Some(nei.append(uppertn.minBy(_._1)._2)) else None
        val lowertn = vlabel.filter(x => x._1 < ts)
        if (lowertn.nonEmpty) Some(nei.append(lowertn.maxBy(_._1)._2)) else None
        //
        val newlab = labelProbability(nei.groupBy(identity))
        if (newlab == tv._2) {count += 1}
        (ts, newlab)
      }
      vertex.setState("lpalabel", newLabel) 
      vertex.messageAllNeighbours((vertex.ID(), newLabel))
      if (count == vlabel.size) {vertex.voteToHalt()}
    }
  }

  override def returnResults(): Any =
    view.getVertices()
      .map(vertex => (vertex.getState[ArrayBuffer[(Long, Long)]]("lpalabel"), vertex.ID()))
      .flatMap(f => f._1.map { x => (x._2, f._2.toString + '_' + x._1.toString) })

  override def extractData(results:ArrayBuffer[Any]):fd ={
    val endResults = results.asInstanceOf[ArrayBuffer[ArrayBuffer[(Long, String)]]].flatten
    try {
      val grouped = endResults.groupBy(f => f._1).mapValues(f=>f.map(_._2))
      val groupedNonIslands = grouped.filter(x => x._2.size > 1)
      val biggest = grouped.maxBy(_._2.size)._2.size
      val sorted = groupedNonIslands.toArray.sortBy(_._2.size)(sortOrdering).map(x => x._2.size)
      val top5 = if (sorted.length <= 10) sorted else sorted.take(10)
      val communities = grouped.map(x => x._2).toArray
      val total = grouped.size
      val totalWithoutIslands = groupedNonIslands.size
      val totalIslands = total - totalWithoutIslands
      val proportion = biggest.toFloat / grouped.map(x => x._2.size).sum
      val totalGT2 = grouped.count(x => x._2.size > 2)
      fd(top5, total, totalIslands, proportion, totalGT2, communities)
    } catch {
      case e: UnsupportedOperationException => fd(Array(0),0,0,0,0, Array(ArrayBuffer("0")))
    }
  }

  override def defineMaxSteps(): Int = 100

}
