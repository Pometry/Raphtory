package com.raphtory.algorithms.generic

import com.raphtory.algorithms.api._
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Table
import com.raphtory.algorithms.api.algorithm.GenericAlgorithm
import com.raphtory.algorithms.api.algorithm.Identity

/**
  *  {s}`CBOD(label: String = "community", cutoff: Double = 0.0, labeler:GraphAlgorithm = Identity())`
  *  : Returns outliers detected based on the community structure of the Graph.
  *
  *  The algorithm assumes that the state of each vertex contains a community label
  *  (e.g., set by running [LPA](com.raphtory.algorithms.generic.community.LPA) on the graph, initially)
  *  and then defines an outlier score based on a node's
  *  community membership and how it compares to its neighbors community memberships.
  *
  * ## Parameters
  *
  *  {s}`label: String = "community"`
  *  : Identifier for community label (default: "community")
  *
  *  {s}`cutoff: Double = 0.0`
  *  : Outlier score threshold (default: 0.0). Identifies the outliers with an outlier score > cutoff.
  *
  *  {s}`labeler: GraphAlgorithm`
  *  : Community algorithm to run to get labels (does nothing by default, i.e., labels should
  *    be already set on the input graph, either via chaining or defined as properties of the data)
  *
  * ## States
  *
  *  {s}`outlierscore: Double`
  *  : Community-based outlier score for vertex
  *
  * ## Returns
  *
  *  (only for vertex such that `outlierscore >= cutoff`)
  *
  *  | vertex name      | outlier score            |
  *  |------------------|--------------------------|
  *  |{s}`name: String` | {s}`outlierscore: Double`|
  *
  * ```{seealso}
  * [](com.raphtory.algorithms.generic.community.LPA)
  * ```
  */
class CBOD(
    label: String = "community",
    cutoff: Double = 0.0,
    labeler: GenericAlgorithm = Identity()
) extends GenericAlgorithm {

  // Run CBOD algorithm and sets "outlierscore" state
  override def apply(graph: GraphPerspective): graph.Graph =
    labeler(graph)
      .step { vertex => //Get neighbors' labels
        val vlabel = vertex.getState[Long](key = label, includeProperties = true)
        vertex.messageAllNeighbours(vlabel)
      }
      .step { v => // Get outlier score
        val vlabel         = v.getState[Long](key = label, includeProperties = true)
        val neighborLabels = v.messageQueue[Long]
        val outlierScore   = 1 - (neighborLabels.count(_ == vlabel) / neighborLabels.length.toDouble)
        v.setState("outlierscore", outlierScore)
      }

  // extract vertex ID and outlier score for vertices with outlierscore >= threshold
  override def tabularise(graph: GraphPerspective): Table =
    graph
      .select { vertex =>
        Row(
                vertex.name(),
                vertex.getStateOrElse[Double]("outlierscore", 10.0)
        )
      }
      .filter(_.get(1).asInstanceOf[Double] >= cutoff)
}

object CBOD {

  def apply(
      label: String = "community",
      cutoff: Double = 0.0,
      labeler: GenericAlgorithm = Identity()
  ) =
    new CBOD(label, cutoff, labeler)
}
