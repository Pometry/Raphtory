package com.raphtory.algorithms

import com.raphtory.core.analysis.entity.Vertex

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
                  (Default: 1) If "average", the weights are assigned based on an average of the neighborhood of two layers.

Returns
  total (Int)     – Number of detected communities.
  communities (List(List(Long))) – Communities sorted by their sizes. Returns largest top communities if specified.

Notes
  This implementation is based on LPA, which incorporates probabilistic elements; This makes it non-deterministic i.e.
  The returned communities may differ on multiple executions.
  **/
object MultilayerLPA {
  def apply(args: Array[String]): MultilayerLPA = new MultilayerLPA(args)
}

class MultilayerLPA(args: Array[String]) extends LPA(args) {
  //args = [top, weight, maxiter, start, end, layer-size, omega]
  val snapshotSize: Long        = args(5).toLong
  val startTime: Long           = args(3).toLong
  val endTime: Long             = args(4).toLong
  val snapshots: Iterable[Long] = for (ts <- startTime to endTime by snapshotSize) yield ts
  val omega: String             = if (arg.length < 7) "1" else args(6)

  override def setup(): Unit =
    view.getVertices().foreach { vertex =>
      // Assign random labels for all instances in time of a vertex as Map(ts, lab)
      val tlabels =
        snapshots
          .filter(ts => vertex.aliveAtWithWindow(ts, snapshotSize))
          .map(ts => (ts, rnd.nextLong()))
          .toList
      vertex.setState("mlpalabel", tlabels)
      val message = (vertex.ID(), tlabels.map(x => (x._1, x._2)))
      vertex.messageAllNeighbours(message)
    }

  override def analyse(): Unit = {
    try view.getMessagedVertices().foreach { vertex =>
      val vlabel    = vertex.getState[List[(Long, Long)]]("mlpalabel").toMap
      val msgQueue  = vertex.messageQueue[(Long, List[(Long, Long)])]
      var voteStatus = vertex.getOrSetState[Boolean]("vote", false)
      var voteCount = 0
      val newLabel = vlabel.map { tv =>
        val ts = tv._1
        val Curlab = tv._2

        // Get weights/labels of neighbours of vertex at time ts
        val nei_ts_freq = weightFunction(vertex, ts) // ID -> freq
        var newlab = if (nei_ts_freq.nonEmpty) {
          val nei_labs = msgQueue
            .filter(x => nei_ts_freq.keySet.contains(x._1)) // filter messages from neighbours at time ts only
            .map { msg =>
              val freq     = nei_ts_freq(msg._1)
              val label_ts = msg._2.filter(_._1 == ts).head._2
              (label_ts, freq) //get label and its frequency at time ts -> (lab, freq)
            }

          //Get labels of past/future instances of vertex
          if (vlabel.contains(ts - snapshotSize))
            nei_labs ++ List((vlabel(ts - snapshotSize), interLayerWeights(omega, vertex, ts - snapshotSize)))
          if (vlabel.contains(ts + snapshotSize))
            nei_labs ++ List((vlabel(ts + snapshotSize), interLayerWeights(omega, vertex, ts)))

          // Get label most prominent in neighborhood of vertex
            val max_freq = nei_labs.groupBy(_._1).mapValues(_.map(_._2).sum)
            max_freq.filter(_._2 == max_freq.values.max).keySet.max
        } else Curlab


        if (newlab == Curlab)
          voteCount += 1

        newlab = if (rnd.nextFloat() < SP) Curlab else newlab
        (ts, newlab)
      }.toList

      // Update node label and broadcast
      vertex.setState("mlpalabel", newLabel)
      val message = (vertex.ID(), newLabel)
      vertex.messageAllNeighbours(message)

      // Update vote status
      voteStatus = if (voteStatus || (voteCount == vlabel.size)) {
        vertex.voteToHalt()
        true
      } else
        false
      vertex.setState("vote", voteStatus)

    } catch {
      case e: Exception => println("Something went wrong with mLPA!", e)
    }
  }
  def interLayerWeights(x: String, v: Vertex, ts: Long): Float =
    x match {
      case "average" =>
        val neilabs = weightFunction(v, ts)
        neilabs.values.sum / neilabs.size
      case _ => omega.toFloat
    }

  def weightFunction(v: Vertex, ts: Long): ParMap[Long, Float] =
    (v.getInCEdgesBetween(ts - snapshotSize, ts) ++ v.getOutEdgesBetween(ts - snapshotSize, ts))
      .map(e => (e.ID(), e.getPropertyValue(weight).getOrElse(1.0F).asInstanceOf[Float]))
      .groupBy(_._1)
      .mapValues(x => x.map(_._2).sum / x.size) // (ID -> Freq)

  override def returnResults(): Any =
    view
      .getVertices()
      .map(vertex =>
        (
          vertex.getState[List[(Long, Long)]]("mlpalabel"),
          vertex.getPropertyValue("Word").getOrElse(vertex.ID()).toString
        )
      )
      .flatMap{f =>
       f._1.map(x => (x._2, f._2 + "_" + x._1.toString))}
      .groupBy(f => f._1)
      .map(f => (f._1, f._2.map(_._2).toList))

}
