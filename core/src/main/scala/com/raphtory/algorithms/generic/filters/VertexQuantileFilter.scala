package com.raphtory.algorithms.generic.filters

import com.raphtory.algorithms.api.Bounded
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Identity
import com.raphtory.algorithms.generic.NodeList

import scala.collection.Searching.Found
import scala.collection.Searching.InsertionPoint
import scala.language.implicitConversions
import scala.math.Numeric.Implicits.infixNumericOps
import scala.reflect.ClassTag

/**
  * {s}`EdqeQuantileFilter()`
  * : Filtered view of the graph based on edge weight
  *
  *  This creates a filtered view of the graph where nodes are removed based on where their value of a given property/state lies in the global
  *  distribution of vertex states, understood in this algorithm as percentiles. For example, one can create a view of the
  *  graph including only the top 50% of vertices in terms of (say) degree. This algorithm does not return any output and is best used in composition
  *  with other algorithms, using the Chain API. The algorithm is agnostic to whether the network is directed or undirected.
  *
  * ## Parameters
  *
  *  {s}`lower: Float = 0.0f`
  *  : The lower cutoff percentile below which nodes will be removed, a float with 0 <= lower <= upper <=1.0 with default value 0.0f
  *
  *  {s}`upper: Float = 1.0f`
  *  : The upper cutoff percentile above which nodes will be removed, a float with 0 <= lower <= upper <=1.0 with default value 1.0f
  *
  *  {s}`weightString: String = "weight"`
  *  : String name of the property/state, defaulting to "weight". As with other weighted algorithms in Raphtory, if no weight property
  *  is there but multi-edges are present, the number of occurrences of each edge is treated as the weight.
  *
  *  {s}`lowerExclusive: Boolean = "false"`
  *  : whether the inequality on the threshold is strict or not at the lower end
  *
  *  {s}`upperExclusive: Boolean = "false"`
  *  : whether the inequality on the= threshold is strict or not at the upper end
  *
  *  {s}`noBins: Int = 1000`
  *  : Number of bins to be used in the histogram. The more the bins, the more precise the thresholds can be (depending on the underlying
  *  distribution of the node property data) but the bigger the array being broadcast.
  *
  * ```{seealso}
  * [](com.raphtory.algorithms.generic.filters.EdgeQuantileFilter)
  * ```
  */

class VertexQuantileFilter[T: Numeric: Bounded: ClassTag](
    lower: Float = 0.0f,
    upper: Float = 1.0f,
    propertyString: String = "weight",
    lowerExclusive: Boolean = false,
    upperExclusive: Boolean = false,
    noBins: Int = 1000
) extends NodeList() {

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
        state.newMin("propertyMin", retainState = true)
        state.newMax("propertyMax", retainState = true)
        state.newHistogram("propertyDist", noBins = noBins, retainState = true)
        state.newAdder[Int]("nodeCount", retainState = true)
      }
      .step { (vertex, state) =>
        state("propertyMin") += vertex.getState(propertyString, true)
        state("propertyMax") += vertex.getState(propertyString, true)
        state("nodeCount") += 1
      }

      // Populate histogram with weights
      .step { (vertex, state) =>
        val toAdd        = Array.fill(noBins)(0)
        val (wMin, wMax) = (state[T]("propertyMin").value, state[T]("propertyMax").value)
        var binNumber    = (noBins * (vertex
          .getState[T](propertyString, true) - wMin).toFloat / (wMax - wMin).toFloat).floor.toInt
        if (binNumber == noBins)
          binNumber -= 1
        toAdd(binNumber) += 1
        state("propertyDist") += toAdd
      }

      // Turn into a cdf for finding quantiles
      .setGlobalState { state =>
        val hist   = state[Array[Int]]("propertyDist").value
        val cumSum = hist.scanLeft(0)(_ + _)
        val nc     = state[Int]("nodeCount").value

        val lowerBin = cumSum.search((nc * lower).floor.toInt) match {
          case InsertionPoint(insertionPoint) => insertionPoint - 1
          case Found(foundIndex)              => foundIndex
        }

        val upperBin = cumSum.search((nc * upper).floor.toInt) match {
          case InsertionPoint(insertionPoint) => insertionPoint
          case Found(foundIndex)              => foundIndex
        }

        val (wMin, wMax) = (state[T]("propertyMin").value, state[T]("propertyMax").value)
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
      .filter((vertex, state) =>
        (if (lowerExclusive)
           vertex.getState[T](propertyString, true).toFloat > state[Float]("lowerThreshold").value
         else
           vertex.getState[T](propertyString, true).toFloat >= state[Float](
                   "lowerThreshold"
           ).value)
          && (if (upperExclusive)
                vertex.getState[T](propertyString, true).toFloat < state[Float](
                        "upperThreshold"
                ).value
              else
                vertex.getState[T](propertyString, true).toFloat <= state[Float](
                        "upperThreshold"
                ).value)
      )
  }
}

object VertexQuantileFilter {

  def apply[T: Numeric: Bounded: ClassTag](
      lower: Float = 0.0f,
      upper: Float = 1.0f,
      propertyString: String = "weight",
      lowerExclusive: Boolean = false,
      upperExclusive: Boolean = false,
      noBins: Int = 1000
  ) =
    new VertexQuantileFilter[T](
            lower,
            upper,
            propertyString,
            lowerExclusive,
            upperExclusive,
            noBins
    )
}
