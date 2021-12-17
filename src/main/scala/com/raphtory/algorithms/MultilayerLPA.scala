package com.raphtory.algorithms
import com.raphtory.core.model.algorithm.GraphAlgorithm
import com.raphtory.core.model.algorithm.GraphPerspective
import com.raphtory.core.model.algorithm.Row
import com.raphtory.core.model.graph.visitor.Vertex

import scala.collection.mutable.{ListBuffer, Queue}
import scala.util.Random

/**
Description
  This returns the communities of the constructed multi-layer graph as detected by synchronous label propagation.
  This transforms the graph into a multi-layer graph where the same vertices on different layers are handled as
  distinct vertices. The algorithm then runs a version of LPA on this view of the graph and returns communities that
  share the same label that can span both vertices on the same layer and other layers.

Parameters
  top (Int) : The number of top largest communities to return. (default: 0) If not specified, Raphtory will return all detected communities.
  weight (String) : Edge property (default: ""). To be specified in case of weighted graph.
  maxIter (Int) : Maximum iterations for LPA to run. (default: 500)
  layers (List(Long)) : List of layer timestamps.
  layerSize (Long) : Size of a single layer that spans all events occurring within this period.
  omega (Double) : Weight of temporal edge that are created between two layers for two persisting instances of a node. (Default: 1.0) If "-1", the weights are assigned based on an average of the neighborhood of two layers.
  seed (Long) : Seed for random elements. (Default: -1).
  output (String) : Directory path that points to where to store the results. (Default: "/tmp/mLPA")

Returns
  total (Int)     – Number of detected communities.
  communities (List(List(Long))) – Communities sorted by their sizes. Returns largest top communities if specified.

Notes
  This implementation is based on LPA, which incorporates probabilistic elements;
  This makes it non-deterministic i.e.
  The returned communities may differ on multiple executions.
  You can set a seed if you want to make this deterministic.
**/
class MultilayerLPA(
                     top: Int = 0,
                     weight: String = "",
                     maxIter: Int = 500,
                     layers: List[Long],
                     layerSize: Long,
                     omega: Double = 1.0,
                     seed: Long = -1,
                     output: String = "/tmp/mLPA"
) extends GraphAlgorithm {

  val rnd: Random               = if (seed == -1) new scala.util.Random else new scala.util.Random(seed)
  val SP                        = 0.2f // Stickiness probability

  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .step { vertex =>
        // Assign random labels for all instances in time of a vertex as Map(ts, lab)
        val tlabels =
          layers.filter(ts => vertex.aliveAt(ts, layerSize)).map(ts => (ts, rnd.nextLong()))
        vertex.setState("mlpalabel", tlabels)
        val message = (vertex.ID(), tlabels.map(x => (x._1, x._2)))
        vertex.messageAllNeighbours(message)
      }
      .iterate(
              { vertex =>
                val vlabel     = vertex.getState[List[(Long, Long)]]("mlpalabel").toMap
                val msgQueue   = vertex.messageQueue[(Long, List[(Long, Long)])]
                var voteStatus = vertex.getOrSetState[Boolean]("vote", false)
                var voteCount  = 0
                val newLabel = vlabel.map {
                  tv =>
                    val ts     = tv._1
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
                        }.to[ListBuffer]

                      //Get labels of past/future instances of vertex
                      if (vlabel.contains(ts - layerSize)) {
                        nei_labs += ((vlabel(ts - layerSize), interLayerWeights(omega, vertex, ts - layerSize)))
                      }

                      if (vlabel.contains(ts + layerSize)) {
                        nei_labs += ((vlabel(ts + layerSize), interLayerWeights(omega, vertex, ts)))
                      }

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

              },
              maxIter,
              true
      )
      .select { vertex =>
        Row(
            vertex.getProperty("name").getOrElse(vertex.ID()).toString,
            vertex.getState("mlpalabel")
        )
      }
      .explode{
        row =>
          row.get(1).asInstanceOf[List[(Long, Long)]]
            .map { lts =>
              Row(lts._2, row.get(0) + "_" + lts._1.toString)
            }
      }
      .writeTo(output)

    def interLayerWeights(omega: Double, v: Vertex, ts: Long): Float =
      omega match {
        case -1 =>
          val neilabs = weightFunction(v, ts)
          neilabs.values.sum / neilabs.size
        case _ => omega.toFloat
      }

    def weightFunction(v: Vertex, ts: Long): Map[Long, Float] =
      (v.getInEdges(after = ts - layerSize, before = ts) ++ v.getOutEdges(after = ts - layerSize, before = ts))
        .map(e => (e.ID(), e.getProperty(weight).getOrElse(1.0f)))
        .groupBy(_._1)
        .mapValues(x => x.map(_._2).sum / x.size) // (ID -> Freq)
  }
}
object MultilayerLPA {
  def apply(
      top: Int = 0,
      weight: String = "",
      maxIter: Int = 500,
      layers: List[Long],
      layerSize: Long,
      omega: Double = 1.0,
      seed: Long = -1,
      output: String = "/tmp/mLPA"
  ) =
    new MultilayerLPA(top, weight, maxIter, layers,layerSize, omega, seed, output)
}