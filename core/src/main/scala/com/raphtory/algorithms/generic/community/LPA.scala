package com.raphtory.algorithms.generic.community

import com.raphtory.algorithms.generic.NodeList
import com.raphtory.algorithms.generic.community.LPA.lpa
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table
import com.raphtory.api.analysis.visitor.Vertex

import scala.util.Random

/**
  * {s}`LPA(weight:String = "", maxIter:Int = 500, seed:Long = -1)`
  *    : run synchronous label propagation based community detection
  *
  *   LPA returns the communities of the constructed graph as detected by synchronous label propagation.
  *   Every vertex is assigned an initial label at random. Looking at the labels of its neighbours, a probability is assigned
  *   to observed labels following an increasing function then the vertex’s label is updated with the label with the highest
  *   probability. If the new label is the same as the current label, the vertex votes to halt. This process iterates until
  *   all vertex labels have converged. The algorithm is synchronous since every vertex updates its label at the same time.
  *
  * ## Parameters
  *
  *   {s}`weight: String = ""`
  *    : Edge weight property. To be specified in case of weighted graph.
  *
  *   {s}`maxIter: Int = 500`
  *    : Maximum iterations for algorithm to run.
  *
  *   {s}`seed: Long`
  *    : Value used for the random selection, can be set to ensure same result is returned per run.
  *      If not specified, it will generate a random seed.
  *
  * ## States
  *
  *    {s}`community: Long`
  *      : The ID of the community the vertex belongs to
  *
  * ## Returns
  *
  *  | vertex name       | community label      |
  *  | ----------------- | -------------------- |
  *  | {s}`name: String` | {s}`community: Long` |
  *
  * ```{note}
  *   This implementation of LPA incorporates probabilistic elements which makes it
  *   non-deterministic; The returned communities may differ on multiple executions.
  *   Which is why you may want to set the seed if testing.
  * ```
  *
  * ```{seealso}
  * [](com.raphtory.algorithms.generic.community.SLPA), [](com.raphtory.algorithms.temporal.community.MultilayerLPA)
  * ```
  */
class LPA[T: Numeric](weight: String = "", maxIter: Int = 50, seed: Long = -1) extends NodeList(Seq("community")) {

  private val rnd: Random = if (seed == -1) new scala.util.Random else new scala.util.Random(seed)

  private val SP = 0.2f // Stickiness probability

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .step { vertex =>
        val lab = rnd.nextLong()
        vertex.setState("community", lab)
        vertex.messageAllNeighbours((vertex.ID, lab))
      }
      .iterate(vertex => lpa(vertex, weight, SP, rnd), maxIter, false)

  override def tabularise(graph: GraphPerspective): Table =
    graph.select { vertex =>
      Row(
              vertex.name(),
              vertex.getState("community")
      )
    }
  // TODO AGGREGATION STATS - See old code in old dir
}

object LPA {

  def apply[T: Numeric](weight: String = "weight", maxIter: Int = 500, seed: Long = -1) =
    new LPA(weight, maxIter, seed)

  def lpa[T](vertex: Vertex, weight: String, SP: Double, rnd: Random)(implicit
      numeric: Numeric[T]
  ): Unit = {
    val vlabel     = vertex.getState[Long]("community")
    val vneigh     = vertex.getAllEdges()
    val neigh_freq = vneigh
      .map(e => (e.ID, e.weight(weightProperty = weight)))
      .groupBy(_._1)
      .view
      .mapValues(x => numeric.toFloat(x.map(_._2).sum))
    // Process neighbour labels into (label, frequency)
    val gp         = vertex
      .messageQueue[(vertex.IDType, Long)]
      .map(v => (v._2, neigh_freq.getOrElse(v._1, 1.0f)))
    // Get label most prominent in neighborhood of vertex
    val maxlab     = gp.groupBy(_._1).view.mapValues(_.map(_._2).sum)
    var newLabel   = maxlab.filter(_._2 == maxlab.values.max).keySet.max
    // Update node label and broadcast
    if (newLabel == vlabel)
      vertex.voteToHalt()
    newLabel = if (rnd.nextFloat() < SP) vlabel else newLabel
    vertex.setState("community", newLabel)
    vertex.messageAllNeighbours((vertex.ID, newLabel))
  }
}
