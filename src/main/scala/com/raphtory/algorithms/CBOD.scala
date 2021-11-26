package com.raphtory.algorithms
import com.raphtory.algorithms.LPA.lpa
import com.raphtory.core.model.algorithm.{GraphPerspective, Row}

/**
Description
  Returns outliers detected based on the community structure of the Graph.

  Tha algorithm runs an instance of LPA on the graph, initially, and then defines an outlier score based on a node's
  community membership and how it compares to its neighbors community memberships.

Parameters
  weight (String) - Edge property (default: ""). To be specified in case of weighted graph.
  cutoff (Double) - Outlier score threshold (default: 0.0). Identifies the outliers with an outlier score > cutoff.
  maxIter (Int)   - Maximum iterations for LPA to run. (default: 500)

Returns
  outliers Map(Long, Double) – Map of (node, outlier score) sorted by their outlier score.
                  Returns `top` nodes with outlier score higher than `cutoff` if specified.
  **/
class CBOD(weight: String = "", maxIter: Int = 500, cutoff: Double, seed: Long = -1, output: String = "/tmp/CBOD")
        extends LPA {
  override def algorithm(graph: GraphPerspective): Unit =
    graph
      .step { vertex =>
        val lab = rnd.nextLong()
        vertex.setState("lpalabel", lab)
        vertex.messageAllNeighbours((vertex.ID(), lab))
      }
      .iterate(//Run LPA til it converges
        vertex => lpa(vertex, weight, SP, rnd), maxIter, false
      )
      .iterate({ vertex => //Get neighbors' labels
        val vlabel = vertex.getState[Long]("lpalabel")
        vertex.messageAllNeighbours(vlabel)
      }, 1, false)
      .iterate(// Get outlier score
              { v =>
                val vlabel         = v.getState[Long]("lpalabel")
                val neighborLabels = v.messageQueue[Long]
                val outlierScore   = 1 - (neighborLabels.count(_ == vlabel) / neighborLabels.length.toDouble)
                v.setState("outlierscore", outlierScore)
              },
              1,
              false
      )
      .select { vertex =>
        Row(
                vertex.ID(),
                vertex.getStateOrElse[Double]("outlierscore", 10.0)
        )
      }
      .filter(_.get(1).asInstanceOf[Double] >= cutoff)
      .writeTo(output)
}

object CBOD {
  def apply(
      weight: String = "",
      maxIter: Int = 500,
      cutoff: Double = 0.0,
      seed: Long = -1,
      output: String = "/tmp/CBOD"
  ) =
    new CBOD(weight, maxIter, cutoff, seed, output)
}
