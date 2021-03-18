package com.raphtory.dev.algodev

import java.time.LocalDateTime

import com.raphtory.algorithms.LPA
import com.raphtory.core.model.analysis.entityVisitors.VertexVisitor

import scala.collection.parallel.ParMap


class MultiLayerLPA(args: Array[String]) extends LPA(args) {
  //args = [top, weight, maxiter, start, end, layer-size, omega]
  val snapshotSize: Long = args(5).toLong
  val startTime: Long = args(3).toLong * snapshotSize //imlater: change this when done with wsdata
  val endTime: Long = args(4).toLong * snapshotSize
  val snapshots: Iterable[Long] = for (ts <- startTime to endTime by snapshotSize) yield ts
  val omega: String = if (arg.length < 7) "1" else args(6)

  override def setup(): Unit =
    view.getVertices().foreach { vertex =>
      // Assign random labels for all instances in time of a vertex as Map(ts, lab)
      val tlabels =
        snapshots
          .filter(ts => vertex.aliveAtWithWindow(ts, snapshotSize))
//          .map(ts => (ts, (scala.util.Random.nextLong(), scala.util.Random.nextLong()))).toArray
          .map(x => (x, (vertex.ID()+x,vertex.ID()-x))).toArray //IM: remove this after testing
      vertex.setState("mlpalabel", tlabels)
      val message = (vertex.ID(), tlabels.map(x=> (x._1, x._2._2)))
      vertex.messageAllNeighbours(message)
    }

  override def analyse(): Unit = {
    val t1 = System.currentTimeMillis()
    try
      view.getMessagedVertices().foreach { vertex =>
        val vlabel = vertex.getState[Array[(Long, (Long, Long))]]("mlpalabel").toMap
        val msgQueue = vertex.messageQueue[(Long, Array[(Long, Long)])]
        var voteCount = 0
        val newLabel = vlabel.map { tv =>
          val ts = tv._1
          // Get weights/labels of neighbours of vertex at time ts
          val nei_ts_freq = weightFunction(vertex, ts) // ID -> freq
          val nei_labs = msgQueue
            .filter(x => nei_ts_freq.keySet.contains(x._1)) // filter messages from neighbours at time ts only
            .map { msg =>
              val freq = nei_ts_freq(msg._1)
              val label_ts = msg._2.filter(_._1==ts).head._2
              (label_ts, freq) //get label at time ts -> (lab, freq)
            }

          //Get labels of past/future instances of vertex //IMlater: links between non consecutive layers should persist or at least degrade?
          if (vlabel.contains(ts - snapshotSize))
            nei_labs.append((vlabel(ts - snapshotSize)._2, interLayerWeights(omega, vertex, ts - snapshotSize)))
          if (vlabel.contains(ts + snapshotSize))
            nei_labs.append((vlabel(ts + snapshotSize)._2, interLayerWeights(omega, vertex, ts)))

          // Get label most prominent in neighborhood of vertex
          val max_freq = nei_labs.groupBy(_._1).mapValues(_.map(_._2).sum)
          val newlab = max_freq.filter(_._2 == max_freq.values.max).keySet.max

          // Update node label and broadcast
          val Oldlab = tv._2._1
          val Vlab = tv._2._2
          (ts, newlab match {
            case Vlab | Oldlab =>
              voteCount += 1
              (List(Vlab, Oldlab).min, List(Vlab, Oldlab).max)
            case _ => (Vlab, newlab)
          })
        }.toArray

        vertex.setState("mlpalabel", newLabel)
        val message = (vertex.ID(), newLabel.map(x=> (x._1, x._2._2)))
        vertex.messageAllNeighbours(message)

        // Vote to halt if all instances of vertex haven't changed their labels
        if (voteCount == vlabel.size) vertex.voteToHalt()
      }
    catch {
      case e: Exception => println("Something went wrong with mLPA!", e)
    }
  if (workerID == 1) println(s"Superstep: ${view.superStep()}    Time: ${LocalDateTime.now()}   ExecTime: ${System.currentTimeMillis() - t1}")
}

  def interLayerWeights(x: String, v: VertexVisitor, ts: Long): Double =
    x match {
      case "None" =>
        val neilabs = weightFunction(v, ts)             //imlater: a more conceptual implementation of temporal weights
        neilabs.values.sum / neilabs.size
      case _ => omega.toDouble                          //imlater: possibly changing this to Double
    }

  def weightFunction(v: VertexVisitor, ts: Long): ParMap[Long, Double] =
    (v.getInCEdgesBetween(ts - snapshotSize, ts) ++ v.getOutEdgesBetween(ts - snapshotSize, ts))
      .map(e => (e.ID(), e.getPropertyValue(weight).getOrElse(1.0).asInstanceOf[Double])) 
      .groupBy(_._1)
      .mapValues(x => x.map(_._2).sum / x.size) // (ID -> Freq)

  override def returnResults(): Any =
    view
      .getVertices()
      .map(vertex =>
        (
                vertex.getState[Array[(Long, (Long, Long))]]("mlpalabel"),
                vertex.getPropertyValue("Word").getOrElse(vertex.ID()).asInstanceOf[String]
        )
      )
      .flatMap(f => f._1.map(x => (x._2._2, f._2 + "_" + x._1.toString)))
      //      .flatMap(f =>  f._1.toArray.map(x => (x._2._2, "\""+x._1.toString+f._2+"\"")))
      .groupBy(f => f._1)
      .map(f => (f._1, f._2.map(_._2)))
}
