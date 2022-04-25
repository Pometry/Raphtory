package com.raphtory.algorithms.generic.filters

import com.raphtory.algorithms.api.Bounded
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Identity

import scala.collection.Searching.Found
import scala.collection.Searching.InsertionPoint
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
  * [](com.raphtory.algorithms.generic.filters.EdgeFilter)
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
) extends Identity() {

  override def apply(graph: GraphPerspective): GraphPerspective = {
    // Check inputs are sound
    if (lower < 0.0f || upper > 1.0f || lower > upper) {
      println("Lower and upper quantiles must be a floats with 0 <= lower < upper <= 1.0")
      logger.error("Lower and upper quantiles must be a floats with 0 <= lower < upper <= 1.0")
      sys.exit(-1)
    }

    // Get minimum and maximum edge weights for histogram creation
    graph
      .setGlobalState { state =>
        state.newMin("weightMin", retainState = true)
        state.newMax("weightMax", retainState = true)
        state.newHistogram("weightDist", noBins = noBins, retainState = true)
        state.newAdder[Int]("edgeCount", retainState = true)
      }
      .step { (vertex, state) =>
        vertex.getOutEdges().foreach { edge =>
          state("weightMin") += edge.weight[T](weightString)
          state("weightMax") += edge.weight[T](weightString)
          state("edgeCount") += 1
        }
      }

      // Populate histogram with weights
      .step { (vertex, state) =>
        val toAdd        = Array.fill(noBins)(0)
        val (wMin, wMax) = (state[T]("weightMin").value, state[T]("weightMax").value)
        vertex.getOutEdges().foreach { edge =>
          var binNumber = (noBins * (edge
            .weight[T](weightString) - wMin).toFloat / (wMax - wMin).toFloat).floor.toInt
          if (binNumber == noBins)
            binNumber -= 1
          toAdd(binNumber) += 1
        }
        state("weightDist") += toAdd
      }

      // Turn into a cdf for finding quantiles
      .setGlobalState { state =>
        val hist   = state[Array[Int]]("weightDist").value
        val cumSum = hist.scanLeft(0)(_ + _)
        val nc     = state[Int]("edgeCount").value

        val lowerBin = cumSum.search((nc * lower).floor.toInt) match {
          case InsertionPoint(insertionPoint) => insertionPoint - 1
          case Found(foundIndex)              => foundIndex
        }

        val upperBin = cumSum.search((nc * upper).floor.toInt) match {
          case InsertionPoint(insertionPoint) => insertionPoint
          case Found(foundIndex)              => foundIndex
        }

        val (wMin, wMax) = (state[T]("weightMin").value, state[T]("weightMax").value)
        state.newConstant[Float](
                "lowerThreshold",
                wMin.toFloat + (wMax - wMin).toFloat / noBins * lowerBin
        )
        state.newConstant[Float](
                "upperThreshold",
                wMin.toFloat + (wMax - wMin).toFloat / noBins * upperBin
        )
      }

      // Finally remove edges that fall outside these quantiles
      .edgeFilter(
              (edge, state) =>
                (if (lowerExclusive)
                   edge.weight[T](weightString).toFloat > state[Float]("lowerThreshold").value
                 else edge.weight[T](weightString).toFloat >= state[Float]("lowerThreshold").value)
                  && (
                          if (upperExclusive)
                            edge.weight[T](weightString).toFloat < state[Float](
                                    "upperThreshold"
                            ).value
                          else
                            edge.weight[T](weightString).toFloat <= state[Float](
                                    "upperThreshold"
                            ).value
                  ),
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
