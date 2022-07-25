package com.raphtory.algorithms.filters

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphstate.Histogram
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.utils.Bounded

import scala.language.implicitConversions
import scala.math.Numeric.Implicits.infixNumericOps
import scala.reflect.ClassTag

/**
  * {s}`EdqeQuantileFilter()`
  * : Filtered view of the graph based on edge weight
  *
  *  This creates a filtered view of the graph where edges are removed based on where their weight value lays in the global
  *  distribution of edge weights, understood in this algorithm as percentiles. For example, one can create a view of the
  *  graph including only the top 50% of edges. This algorithm does not return any output and is best used in composition
  *  with other algorithms, using the Chain API. The algorithm treats the network as directed.
  *
  * ## Parameters
  *
  *  {s}`lower: Float = 0.0f`
  *  : The lower cutoff percentile below which edges will be removed, a float with 0 <= lower <= upper <=1.0 with default value 0.0f
  *
  *  {s}`upper: Float = 1.0f`
  *  : The upper cutoff percentile above which edges will be removed, a float with 0 <= lower <= upper <=1.0 with default value 1.0f
  *
  *  {s}`weightString: String = "weight"`
  *  : String name of the weight property, defaulting to "weight". As with other weighted algorithms in Raphtory, if no weight property
  *  is there but multi-edges are present, the number of occurrences of each edge is treated as the weight.
  *
  *  {s}`lowerExclusive: Boolean = "false"`
  *  : whether the inequality on the edge weight threshold is strict or not at the lower end
  *
  *  {s}`upperExclusive: Boolean = "false"`
  *  : whether the inequality on the edge weight threshold is strict or not at the upper end
  *
  *  {s}`noBins: Int = 1000`
  *  : Number of bins to be used in the histogram. The more the bins, the more precise the thresholds can be (depending on the underlying
  *  distribution of the edge weight data) but the bigger the array being broadcast.
  *
  *  {s}`pruneNodes: Boolean = true`
  *  : if set to true, nodes which are left without any incoming or outgoing edges by the end of this filtering are also pruned from the graph.
  *
  * ```{seealso}
  * [](com.raphtory.algorithms.filters.EdgeFilter)
  * ```
  */

class EdgeQuantileFilter[T: Numeric: Bounded: ClassTag](
    lower: Float = 0.0f,
    upper: Float = 1.0f,
    weightString: String = "weight",
    lowerExclusive: Boolean = false,
    upperExclusive: Boolean = false,
    noBins: Int = 1000,
    pruneNodes: Boolean = true
) extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph = {
    // Check inputs are sound
    if (lower < 0.0f || upper > 1.0f || lower > upper) {
      logger.error("Lower and upper quantiles must be a floats with 0 <= lower < upper <= 1.0")
      return graph.identity
    }

    // Get minimum and maximum edge weights for histogram creation
    graph
      .setGlobalState { state =>
        state.newMin[T]("weightMin", retainState = true)
        state.newMax[T]("weightMax", retainState = true)
      }
      .step { (vertex, state) =>
        vertex.outEdges.foreach { edge =>
          state("weightMin") += edge.weight[T](weightString)
          state("weightMax") += edge.weight[T](weightString)
        }
      }
      .setGlobalState { state =>
        val minWeight: T = state("weightMin").value
        val maxWeight: T = state("weightMax").value
        state.newHistogram[T]("weightDist", noBins = noBins, minWeight, maxWeight)
      }

      // Populate histogram with weights
      .step { (vertex, state) =>
        val histogram = state("weightDist")
        vertex.outEdges.foreach { edge =>
          histogram += (edge.weight(weightString))
        }
      }
      .setGlobalState { state =>
        val histogram: Histogram[T] = state("weightDist").value
        state.newConstant[Float]("upperQuantile", histogram.quantile(upper))
        state.newConstant[Float]("lowerQuantile", histogram.quantile(lower))
      }

      // Finally remove edges that fall outside these quantiles
      .edgeFilter(
              (edge, state) => {
                val edgeWeight                  = edge.weight[T](weightString).toFloat
                val upperQuantile: Float        = state("upperQuantile").value
                val lowerQuantile: Float        = state("lowerQuantile").value
                val lowerExclusiveTest: Boolean =
                  if (lowerExclusive)
                    edgeWeight > lowerQuantile
                  else
                    edgeWeight >= lowerQuantile

                val upperExclusiveTest: Boolean =
                  if (upperExclusive)
                    edgeWeight < upperQuantile
                  else
                    edgeWeight <= upperQuantile

                lowerExclusiveTest && upperExclusiveTest
              },
              pruneNodes = pruneNodes
      )
  }
}

object EdgeQuantileFilter {

  def apply[T: Numeric: Bounded: ClassTag](
      lower: Float = 0.0f,
      upper: Float = 1.0f,
      weightString: String = "weight",
      lowerExclusive: Boolean = false,
      upperExclusive: Boolean = false,
      noBins: Int = 1000,
      pruneNodes: Boolean = true
  ) =
    new EdgeQuantileFilter[T](
            lower,
            upper,
            weightString,
            lowerExclusive,
            upperExclusive,
            noBins,
            pruneNodes
    )
}
