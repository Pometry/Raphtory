package com.raphtory.dev.algorithms

import com.raphtory.algorithms.LPA
import com.raphtory.core.analysis.entity.Vertex

class MultiLayerLPA2(args: Array[String]) extends LPA(args) {
  //args = [top, weight, maxiter, start, end, layer-size, omega, stickiness prob, scaled, label initial prob]
  val snapshotSize: Long        = args(5).toLong
  val startTime: Long           = args(3).toLong
  val endTime: Long             = args(4).toLong
  val snapshots: Iterable[Long] = for (ts <- startTime to endTime by snapshotSize) yield ts
  val omega: String             = if (arg.length < 7) "1" else args(6)
  override val SP: Float        = if (arg.length < 8) 0.2f else args(7).toFloat
  val scaled: Boolean           = if (arg.length < 9) true else args(8).toBoolean
  val commprob: Float           = if (arg.length < 10) 1.0f else args(9).toFloat
  var countvote: Long = 0L

  var lastManStanding = (0L, -1L, -1L)
  override def setup(): Unit =
    view.getVertices().foreach { vertex =>
      // Assign random labels for all instances in time of a vertex as Map(ts, lab)
      val slabel = rnd.nextLong()
      val prob   = rnd.nextFloat()
      val tlabels =
        snapshots
          .filter(ts => vertex.aliveAtWithWindow(ts, snapshotSize))
          //          .map(ts => (ts, rnd.nextLong()))
          .map(ts => (ts, if (prob < commprob) slabel else rnd.nextLong()))
          .toArray
      vertex.setState("mlpalabel", tlabels)
      val message = (vertex.ID(), tlabels.map(x => (x._1, x._2)).toList)
      vertex.messageAllNeighbours(message)
    }

  override def analyse(): Unit = {
    lastManStanding = (0L, -1L, -1L)
    val messaged = view.getMessagedVertices()
    messaged.foreach { vertex =>
      val vlabel     = vertex.getState[Array[(Long, Long)]]("mlpalabel").toMap
      val msgQueue   = vertex.messageQueue[(Long, List[(Long, Long)])]
      var voteStatus = vertex.getOrSetState[Boolean]("vote", false)
      var voteCount  = 0
      val newLabel   = vlabel.map { tv =>
          val ts     = tv._1
          val Curlab = tv._2
          // Get weights/labels of neighbours of vertex at time ts
          val nei_ts_freq = weightFunction(vertex, ts) // ID -> freq
          var newlab = if (nei_ts_freq.nonEmpty) { //im: put this a bit lower
            val nei_labs = msgQueue
              .filter(x => nei_ts_freq.keySet.contains(x._1)) // filter messages from neighbours at time ts only
              .map { msg =>
                val freq     = nei_ts_freq(msg._1)
                val label_ts = msg._2.filter(_._1 == ts).head._2
                (label_ts, freq) //get label at time ts -> (lab, freq)
              }

            if (vlabel.contains(ts - snapshotSize))
              nei_labs ++ List((vlabel(ts - snapshotSize), interLayerWeights(omega, vertex, ts - snapshotSize)))
            if (vlabel.contains(ts + snapshotSize))
              nei_labs ++ List((vlabel(ts + snapshotSize), interLayerWeights(omega, vertex, ts)))
            // Get label most prominent in neighborhood of vertex
            selectiveProc(v = vertex, ts, gp = nei_labs.map(_._1).toArray)
            val max_freq = nei_labs.groupBy(_._1).mapValues(_.map(_._2).sum)
            max_freq.filter(_._2 == max_freq.values.max).keySet.max
          } else Curlab

          // Update node label and broadcast
          val sprob = rnd.nextFloat()
          if (newlab == Curlab)
            voteCount += 1
          else
            lastManStanding = (vertex.ID(), Curlab, newlab)

        newlab = if (sprob < SP) Curlab else newlab
        (ts, newlab)
        }.toArray

      vertex.setState("mlpalabel", newLabel)
      val message = (vertex.ID(), newLabel.toList)
      vertex.messageAllNeighbours(message)

      // Vote to halt if all instances of vertex haven't changed their labels
      voteStatus = if (voteStatus || (voteCount == vlabel.size)) {
        vertex.voteToHalt()
        true
      } else {
        countvote += 1L
        false
      }
      vertex.setState("vote", voteStatus)
    }
    if (countvote == 1)
      println(
              s"WiD: $workerID \t Still processing: $countvote / ${messaged.size} \t Status:$lastManStanding "
      )
    countvote = 0L
  }
  def selectiveProc(v: Vertex, ts: Long, gp: Array[Long]): Unit = {}
  def interLayerWeights(x: String, v: Vertex, ts: Long): Float =
    x match {
      case "average" =>
        val neilabs = weightFunction(v, ts)
        neilabs.values.sum / neilabs.size
      case _ => omega.toFloat
    }

  def weightFunction(v: Vertex, ts: Long): Map[Long, Float] = {
    var nei_weights =
      (v.getInCEdgesBetween(ts - snapshotSize, ts) ++ v.getOutEdgesBetween(ts - snapshotSize, ts)).map(e =>
        (e.ID(), e.getPropertyValue(weight).getOrElse(1.0f).asInstanceOf[Float])
      )
    if (scaled) {
      val scale = scaling(nei_weights.map(_._2).toArray)
      nei_weights = nei_weights.map(x => (x._1, x._2 / scale))

    }
    //    nei_weights =
    //      var nei_filt = nei_weights.toArray.sortBy(-_._2)
    //      nei_filt = nei_filt.take((nei_weights.size*filter).toInt)
    //    nei_weights

    //      nei_filt = if (nei_filt.nonEmpty) nei_filt else nei_weights.toArray.take(1)
    nei_weights.toArray.groupBy(_._1).mapValues(x => x.map(_._2).sum) // (ID -> Freq)
  }

  def scaling(freq: Array[Float]): Float = math.sqrt(freq.map(math.pow(_, 2)).sum).toFloat

  override def returnResults(): Any = {
    println(s"mLPA - wID: $workerID totNodes: ${view.getVertices().size}")
    view
      .getVertices()
      .map(vertex =>
        (
                vertex.getState[Array[(Long, Long)]]("mlpalabel"),
                vertex.getPropertyValue("Word").getOrElse(vertex.ID()).toString
        )
      )
      .flatMap(f => f._1.map(x => (x._2, f._2 + "_" + x._1.toString)))
      //      .flatMap(f =>  f._1.toArray.map(x => (x._2._2, "\""+x._1.toString+f._2+"\"")))
      .groupBy(f => f._1)
      .map(f => (f._1, f._2.map(_._2).toList))
  }
}
