package com.raphtory.algorithms

import com.raphtory.core.model.analysis.entityVisitors.VertexVisitor

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ParMap
import scala.collection.parallel.mutable.{ParArray}

/**
- change arguments order

 **/
class MultiLayerLPAparams(args: Array[String]) extends LPA(args) {
  //args = [top_c, start, end, layer-size, w, theta, property]
  val snapshotSize: Long = args(3).toLong
  val startTime: Long = args(1).toLong * snapshotSize //TODO: im: change this when done with wsdata
  val endTime: Long = args(2).toLong * snapshotSize
  val snapshots: Iterator[Long] = (for (ts <- startTime + snapshotSize to endTime by snapshotSize) yield ts).toIterator // TODO: im: check
  val omega: String = args(4) //todo: im: check change to omega is proper
  val theta: Double = args(5).toDouble
  override val weight: String = args(6)

  override def setup(): Unit =
    view.getVertices().foreach { vertex =>
      // Assign random labels for all instances in time of a vertex as map(ts, lab)
      val tlabels = mutable.TreeMap[Long, List[Long]]()
        snapshots
          .filter(t => vertex.aliveAtWithWindow(t, snapshotSize))
          .foreach(tlabels.put(_, List(-1L, scala.util.Random.nextLong())))
      vertex.setState("lpalabel", tlabels)
      vertex.messageAllNeighbours((vertex.ID(), tlabels))
    }

  override def analyse(): Unit =
    try
      view.getMessagedVertices().foreach { vertex =>
      val vlabel = vertex.getState[mutable.TreeMap[Long, List[Long]]]("lpalabel")
      val msgQueue  = vertex.messageQueue[(Long, mutable.TreeMap[Long, List[Long]])]
      var voteCount = 0
      val newLabel = vlabel.map { tv =>
        val ts          = tv._1

        // Get weights/labels of neighbours of vertex at time ts
        val nei_ts_freq = weightFunction(vertex, ts) // ID -> freq
        val nei_labs = msgQueue
                .filter(x => nei_ts_freq.keySet.contains(x._1))     // filter messages from neighbours at time ts only
                .map { msg =>
                  (msg._2(ts).last, nei_ts_freq(msg._1))        //get lab at time ts -> (lab, freq)
                }

        //HERE
        val v_tempo_nei_labs =
          vlabel.filter(_._1 == ts + snapshotSize).map(x => (x._2.last, interLayerWeights(omega, vertex, ts))) ++
            vlabel
              .filter(_._1 == ts - snapshotSize)
              .map(x => (x._2.last, interLayerWeights(omega, vertex, ts - snapshotSize)))
        nei_labs.appendAll(v_tempo_nei_labs)

        val max_freq = nei_labs.groupBy(_._1).mapValues(_.map(_._2).sum)
        var newlab   = max_freq.filter(f => f._2 == max_freq.values.max).keySet.max

        if (tv._2.contains(newlab)) {
          newlab = tv._2.max
          voteCount += 1
        }
        (ts, tv._2.tail.union(List(newlab)))
      }

      vertex.setState("lpalabel", newLabel)
      vertex.messageAllNeighbours((vertex.ID(), newLabel))
      if (voteCount == vlabel.size) vertex.voteToHalt()
    } catch {
      case e: Exception => println(e)
    }

  def interLayerWeights(x: String, v: VertexVisitor, ts: Long): Long =
    x match {
      case "none" =>
        val neilabs = weightFunction(v, ts)
        neilabs.values.sum / neilabs.size
      case _ => omega.toLong
    }

  def weightFunction(v: VertexVisitor, ts: Long): ParMap[Long, Long] = { // TODO: IM: handle multiple weights with snapshot
    val nei_ts =
      (v.getInCEdgesBetween(ts - snapshotSize, ts) ++ v.getOutEdgesBetween(ts - snapshotSize, ts)).filter(e =>
        e.getPropertyValueAt("ScaledFreq", ts).getOrElse(1.0).asInstanceOf[Double] > theta // TODO: IM: parameterize scaled freq
      )
    nei_ts
      .map(e => (e.ID(), e.getPropertyValueAt(omega, ts).getOrElse(1L).asInstanceOf[Long]))
      .groupBy(_._1)
      .mapValues(x => x.map(_._2).sum) // (ID -> Freq)
  }
  override def returnResults(): Any =
    view
      .getVertices()
      .map(vertex =>
        (
                vertex.getOrSetState[Array[(Long, List[Long])]]("lpalabel", Array(0, -1L)),
                vertex.getPropertyValue("Word").getOrElse("Unknown")
        )
      )
      .flatMap(f => f._1.map(x => (x._2.last, f._2.toString + '_' + x._1.toString)))

  override def extractData(results: ArrayBuffer[Any]): fd = { //TODO:IM: match this to LPA
    val endResults = results.asInstanceOf[ArrayBuffer[ArrayBuffer[(Long, String)]]].flatten
    try {
      val grouped             = endResults.groupBy(f => f._1).mapValues(f => f.map(_._2))
      val groupedNonIslands   = grouped.filter(x => x._2.size > 1)
      val biggest             = grouped.maxBy(_._2.size)._2.size
      val sorted              = groupedNonIslands.toArray.sortBy(_._2.size)(sortOrdering)
      val top5                = sorted.map(x => x._2.size).take(10)
      val total               = grouped.size
      val totalWithoutIslands = groupedNonIslands.size
      val totalIslands        = total - totalWithoutIslands
      val communities         = sorted.map(x => x._2).take(if (top == 0) sorted.length else top)
      fd(top5, total, totalIslands, communities)
    } catch {
      case e: UnsupportedOperationException => fd(Array(0), 0, 0, Array(ArrayBuffer("0")))
    }
  }

  override def defineMaxSteps(): Int = 100

}
