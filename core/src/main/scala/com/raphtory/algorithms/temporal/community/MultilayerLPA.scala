package com.raphtory.algorithms.temporal.community

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.algorithm.GenericReduction
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.graphview.ReducedGraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table
import com.raphtory.api.analysis.visitor.ReducedVertex
import com.raphtory.api.analysis.visitor.Vertex

import scala.util.Random

/**
  * {s}`MultilayerLPA(weight: String = "", maxIter: Int = 500, layers: List[Long], layerSize: Long, omega: Double = 1.0, seed: Long = -1)`
  *  : find multilayer communities using synchronous label propagation
  *
  * This returns the communities of the constructed multi-layer graph as detected by synchronous label propagation.
  * This transforms the graph into a multi-layer graph where the same vertices on different layers are handled as
  * distinct vertices. The algorithm then runs a version of LPA on this view of the graph and returns communities that
  * share the same label that can span both vertices on the same layer and other layers.
  *
  * ## Parameters
  *
  *  {s}`weight: String = ""`
  *    : Edge property to be specified in case of weighted graph.
  *
  *  {s}`maxIter: Int = 500`
  *    : Maximum iterations for LPA to run.
  *
  *  {s}`layers: List[Long]`
  *    : List of layer timestamps.
  *
  *  {s}`layerSize: Long`
  *    : Size of a single layer that spans all events occurring within this period.
  *
  *  {s}`omega: Double = 1.0`
  *    : Weight of temporal edge that are created between two layers for two persisting instances of a node.
  *      If {s}`omega=-1`, the weights are assigned based on an average of the neighborhood of two layers.
  *
  *  {s}`seed: Long`
  *    : Seed for random elements. Defaults to random seed.
  *
  * ## States
  *
  *  {s}`mlpalabel: List[Long, Long]`
  *    : List of community labels for all instances of the vertex of the form (timestamp, label)
  *
  * ## Returns
  *
  *  | community label  | vertex name and timestamp   |
  *  | ---------------- | --------------------------- |
  *  | {s}`label: Long` | {s}`name + "_" + timestamp` |
  *
  * ```{note}
  *   This implementation is based on LPA, which incorporates probabilistic elements. This makes it
  *   non-deterministic i.e., the returned communities may differ on multiple executions.
  *   You can set a seed if you want to make this deterministic.
  * ```
  *
  * ```{seealso}
  * [](com.raphtory.algorithms.generic.community.LPA)
  * ```
  */
class MultilayerLPA(
    weight: String = "",
    maxIter: Int = 500,
    layers: List[Long],
    layerSize: Long,
    omega: Double = 1.0,
    seed: Long = -1
) extends GenericReduction[Row] {

  private val rnd: Random = if (seed == -1) new scala.util.Random else new scala.util.Random(seed)
  private val SP          = 0.2f // Stickiness probability

  override def apply(graph: GraphPerspective): graph.ReducedGraph = {
    def interLayerWeights(omega: Double, v: ReducedVertex, ts: Long): Float =
      omega match {
        case -1 =>
          val neilabs = weightFunction(v, ts)
          neilabs.values.sum / neilabs.size
        case _  => omega.toFloat
      }

    def weightFunction(v: ReducedVertex, ts: Long): Map[v.IDType, Float] =
      (v.getInEdges(after = ts - layerSize, before = ts) ++ v.getOutEdges(
              after = ts - layerSize,
              before = ts
      )).map(e => (e.ID, e.weight(default = 1.0f)))
        .groupBy(_._1)
        .view
        .mapValues(x => x.map(_._2).sum / x.size)
        .toMap // (ID -> Freq)

    graph.reducedView
      .step { vertex =>
        // Assign random labels for all instances in time of a vertex as Map(ts, lab)
        val tlabels =
          layers.filter(ts => vertex.aliveAt(ts, layerSize)).map(ts => (ts, rnd.nextLong()))
        vertex.setState("mlpalabel", tlabels)
        val message = (vertex.ID, tlabels.map(x => (x._1, x._2)))
        vertex.messageAllNeighbours(message)
      }
      .iterate(
              { vertex =>
                val vlabel     = vertex.getState[List[(Long, Long)]]("mlpalabel").toMap
                val msgQueue   = vertex.messageQueue[(vertex.IDType, List[(Long, Long)])]
                var voteStatus = vertex.getOrSetState[Boolean]("vote", false)
                var voteCount  = 0
                val newLabel   = vlabel.map {
                  tv =>
                    val ts     = tv._1
                    val Curlab = tv._2

                    // Get weights/labels of neighbours of vertex at time ts
                    val nei_ts_freq = weightFunction(vertex, ts)
//                      .asInstanceOf[Map[vertex.VertexID, Float]] // ID -> freq
                    var newlab      = if (nei_ts_freq.nonEmpty) {
                      val nei_labs = msgQueue
                        .filter(x =>
                          nei_ts_freq.keySet.contains(x._1)
                        ) // filter messages from neighbours at time ts only
                        .map { msg =>
                          val freq     = nei_ts_freq(msg._1)
                          val label_ts = msg._2.filter(_._1 == ts).head._2
                          (label_ts, freq) //get label and its frequency at time ts -> (lab, freq)
                        }
                        .toBuffer

                      //Get labels of past/future instances of vertex
                      if (vlabel.contains(ts - layerSize))
                        nei_labs += (
                                (
                                        vlabel(ts - layerSize),
                                        interLayerWeights(omega, vertex, ts - layerSize)
                                )
                        )

                      if (vlabel.contains(ts + layerSize))
                        nei_labs += ((vlabel(ts + layerSize), interLayerWeights(omega, vertex, ts)))

                      // Get label most prominent in neighborhood of vertex
                      val max_freq = nei_labs
                        .groupBy[Long](_._1)
                        .view
                        .mapValues(_.map(_._2).sum)
                      max_freq.filter(_._2 == max_freq.values.max).keySet.max
                    }
                    else Curlab

                    if (newlab == Curlab)
                      voteCount += 1

                    newlab = if (rnd.nextFloat() < SP) Curlab else newlab
                    (ts, newlab)
                }.toList

                // Update node label and broadcast
                vertex.setState("mlpalabel", newLabel)
                val message = (vertex.ID, newLabel)
                vertex.messageAllNeighbours(message)

                // Update vote status
                voteStatus = if (voteStatus || (voteCount == vlabel.size)) {
                  vertex.voteToHalt()
                  true
                }
                else
                  false
                vertex.setState("vote", voteStatus)

              },
              maxIter,
              true
      )
  }

  override def tabularise(graph: ReducedGraphPerspective): Table[Row] =
    graph
      .select { vertex =>
        Row(
                vertex.name(),
                vertex.getState("mlpalabel")
        )
      }
      .explode { row =>
        row
          .get(1)
          .asInstanceOf[List[(Long, Long)]]
          .map(lts => Row(lts._2, s"${row.get(0)}_${lts._1}"))
      }

}

object MultilayerLPA {

  def apply(
      weight: String = "",
      maxIter: Int = 500,
      layers: List[Long],
      layerSize: Long,
      omega: Double = 1.0,
      seed: Long = -1
  ) =
    new MultilayerLPA(weight, maxIter, layers, layerSize, omega, seed)
}
