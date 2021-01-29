package com.raphtory.algorithms

import com.raphtory.core.model.analysis.entityVisitors.VertexVisitor

import scala.collection.mutable
import scala.collection.parallel.ParMap

/**
Description
  This returns the communities of the constructed multi-layer graph as detected by synchronous label propagation.

  This transforms the graph into a multi-layer graph where the same vertices on different layers are handled as
  distinct vertices. The algorithm then runs a version of LPA on this view of the graph and returns communities that
  share the same label that can span both vertices on the same layer and other layers.

Parameters
  top (Int)       – The number of top largest communities to return. (default: 0)
                      If not specified, Raphtory will return all detected communities.
  weight (String) - Edge property (default: ""). To be specified in case of weighted graph.
  maxIter (Int)   - Maximum iterations for LPA to run. (default: 500)
  start (Long)    - Oldest time in the graph events.
  end (Long)      - Newest time in the graph events.
  layerSize (Long)- Size of a single layer that spans all events occurring within this period.
  omega (Long)    - Weight of temporal edge that are created between two layers for two persisting instances of a node.
                  (Default: 1) If "None", the weights are assigned based on an average of the neighborhood of two layers.

Returns
  total (Int)     – Number of detected communities.
  communities (List(List(Long))) – Communities sorted by their sizes. Returns largest top communities if specified.

Notes
  This implementation is based on LPA, which incorporates probabilistic elements; This makes it non-deterministic i.e.
  The returned communities may differ on multiple executions.
  **/
object MultiLayerLPA {
  def apply(args: Array[String]): MultiLayerLPA = new MultiLayerLPA(args)
}

class MultiLayerLPA(args: Array[String]) extends LPA(args) {
  //args = [top, weight, maxiter, start, end, layer-size, omega]
  val snapshotSize: Long        = args(5).toLong
  val startTime: Long           = args(3).toLong * snapshotSize //imlater: change this when done with wsdata
  val endTime: Long             = args(4).toLong * snapshotSize
  val snapshots: Iterator[Long] = (for (ts <- startTime to endTime by snapshotSize) yield ts).toIterator
  val omega: String             = if (arg.length < 7) "1" else args(6)

  override def setup(): Unit =
    view.getVertices().foreach { vertex =>
      // Assign random labels for all instances in time of a vertex as Map(ts, lab)
      val tlabels = mutable.TreeMap[Long, (Long, Long)]()
      snapshots
        .filter(t => vertex.aliveAtWithWindow(t, snapshotSize))
        .foreach(tlabels.put(_, (-1L, scala.util.Random.nextLong())))
      vertex.setState("mlpalabel", tlabels)
      vertex.messageAllNeighbours((vertex.ID(), tlabels))
    }

  override def analyse(): Unit =
    try
    view.getMessagedVertices().foreach { vertex =>
      val vlabel    = vertex.getState[mutable.TreeMap[Long, (Long, Long)]]("mlpalabel")
      val msgQueue  = vertex.messageQueue[(Long, mutable.TreeMap[Long, (Long, Long)])]
      var voteCount = 0
      val newLabel = vlabel.map { tv =>
        val ts = tv._1
        // Get weights/labels of neighbours of vertex at time ts
        val nei_ts_freq = weightFunction(vertex, ts) // ID -> freq
        val nei_labs = msgQueue
          .filter(x => nei_ts_freq.keySet.contains(x._1)) // filter messages from neighbours at time ts only
          .map { msg =>
            val a = nei_ts_freq(msg._1)
            (msg._2(ts)._2, a) //get label at time ts -> (lab, freq)
          }

        //Get labels of past/future instances of vertex //IMlater: links between non consecutive layers should persist or at least degrade?
        if (vlabel.contains(ts - snapshotSize))
          nei_labs.append((vlabel(ts - snapshotSize)._2, interLayerWeights(omega, vertex, ts - snapshotSize)))
        if (vlabel.contains(ts + snapshotSize))
          nei_labs.append((vlabel(ts + snapshotSize)._2, interLayerWeights(omega, vertex, ts)))

        // Get label most prominent in neighborhood of vertex
        val max_freq = nei_labs.groupBy(_._1).mapValues(_.map(_._2).sum)
        val newlab   = max_freq.filter(_._2 == max_freq.values.max).keySet.max

        // Update node label and broadcast
        val Oldlab = tv._2._1
        val Vlab   = tv._2._2
        (ts, newlab match {
          case Vlab | Oldlab =>
            voteCount += 1
            (List(Vlab, Oldlab).min, List(Vlab, Oldlab).max)
          case _ => (Vlab, newlab)
        })
      }

      vertex.setState("mlpalabel", newLabel)
      vertex.messageAllNeighbours((vertex.ID(), newLabel))
      // Vote to halt if all instances of vertex haven't changed their labels
      if (voteCount == vlabel.size) vertex.voteToHalt()
//        println(view.superStep())
    }
  catch {
      case e: Exception => println("Something went wrong with mLPA!", e)
    }

  def interLayerWeights(x: String, v: VertexVisitor, ts: Long): Long =
    x match {
      case "None" =>
        val neilabs = weightFunction(v, ts)
        neilabs.values.sum / neilabs.size
      case _ => omega.toLong                            //imlater: possibly changing this to Double
    }

  def weightFunction(v: VertexVisitor, ts: Long): ParMap[Long, Long] =
    (v.getInCEdgesBetween(ts - snapshotSize, ts) ++ v.getOutEdgesBetween(ts - snapshotSize, ts))
      .map(e => (e.ID(), e.getPropertyValue(weight).getOrElse(1L).asInstanceOf[Long])) //  im: fix this one after pulling new changes
      .groupBy(_._1)
      .mapValues(x => x.map(_._2).sum / x.size) // (ID -> Freq)

  override def returnResults(): Any =
    view
      .getVertices()
      .map(vertex =>
        (
                vertex.getState[mutable.TreeMap[Long, (Long, Long)]]("mlpalabel"),
                vertex.getPropertyValue("Word").getOrElse(vertex.ID()).asInstanceOf[String]
        )
      )
      .flatMap(f => f._1.toArray.map(x => (x._2._2, f._2 + "_" + x._1.toString)))
      //      .flatMap(f =>  f._1.toArray.map(x => (x._2._2, "\""+x._1.toString+f._2+"\"")))
      .groupBy(f => f._1)
      .map(f => (f._1, f._2.map(_._2)))
}
