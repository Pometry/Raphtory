package com.raphtory.algorithms

import com.raphtory.algorithms.LPA.lpa
import com.raphtory.core.model.algorithm.{GraphPerspective, Row, Table, GraphAlgorithm, Identity}

/**
Description
  Returns outliers detected based on the community structure of the Graph.

  The algorithm assumes that the state of each vertex contains a community label (e.g., set by running LPA on the graph, initially)
  and then defines an outlier score based on a node's
  community membership and how it compares to its neighbors community memberships.

Parameters
  label (String) - Identifier for community label
  cutoff (Double) - Outlier score threshold (default: 0.0). Identifies the outliers with an outlier score > cutoff.
  labeler (Option[GraphAlgorithm]) - Community algorithm to run to get labels (None by default)

Returns
  outliers Map(Long, Double) – Map of (node, outlier score) sorted by their outlier score.
                  Returns `top` nodes with outlier score higher than `cutoff` if specified.
  **/
class CBOD(label: String = "label", cutoff: Double = 0.0, output: String = "/tmp/CBOD", labeler:GraphAlgorithm = Identity())
        extends GraphAlgorithm {
  override def graphStage(graph: GraphPerspective): GraphPerspective = {
      labeler.graphStage(graph)
      .step { vertex => //Get neighbors' labels
        val vlabel = vertex.getState[Long](key = label)
        vertex.messageAllNeighbours(vlabel)
      }
      .step { v => // Get outlier score
        val vlabel         = v.getState[Long](key = label)
        val neighborLabels = v.messageQueue[Long]
        val outlierScore   = 1 - (neighborLabels.count(_ == vlabel) / neighborLabels.length.toDouble)
        v.setState("outlierscore", outlierScore)
      }
  }

  override def tableStage(graph: GraphPerspective): Table = {
    graph.select { vertex =>
      Row(
        vertex.ID(),
        vertex.getStateOrElse[Double]("outlierscore", 10.0)
      )
    }
      .filter(_.get(1).asInstanceOf[Double] >= cutoff)
  }

  override def write(table: Table): Unit = {
    table.writeTo(output)
  }
}

object CBOD {
  def apply(
      label: String = "label",
      cutoff: Double = 0.0,
      output: String = "/tmp/CBOD",
      labeler: GraphAlgorithm = Identity()
  ) =
    new CBOD(label, cutoff, output, labeler)
}
