package com.raphtory.algorithms.generic.filters

import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Identity

import scala.math.Ordered.orderingToOrdered
import scala.reflect.ClassTag

class EdgeWeightQuantile[T: Numeric: ClassTag](
    lower: Float = 0.0f,
    upper: Float = 1.0f,
    weightString: String = "weight",
    lowerExclusive: Boolean = false,
    upperExclusive: Boolean = false,
    pruneNodes: Boolean = true
) extends Identity() {

  override def apply(graph: GraphPerspective): GraphPerspective = {
    // check inputs are sound
    if (lower < 0.0f || upper > 1.0f || lower > upper)
      logger.error("Lower and upper quantiles must be a floats with 0 <= lower < upper <= 1.0")

    // I'm not proud of this XD
    val opLower =
      if (lowerExclusive) (a: T, b: T) => (a > b) else (a: T, b: T) => (a >= b)
    val opUpper =
      if (upperExclusive) (a: T, b: T) => (a < b) else (a: T, b: T) => (a <= b)

    // array for edge weights
    graph
      .setGlobalState(g =>
        g.newAccumulator(
                "weightList",
                initialValue = Array[T](),
                op = (x: Array[T], y: Array[T]) => x ++ y
        )
      )
      .step((vertex, state) =>
        state("weightList") += vertex.getOutEdges().map(_.weight[T](weightString)).toArray
      )
      .setGlobalState { state =>
        val weights     = state[Array[T]]("weightList").value
        weights.sortInPlace()
        val lower_index = ((weights.size - 1) * lower).floor.toInt
        val upper_index = ((weights.size - 1) * upper).floor.toInt

        state.newAdder(
                name = "lowerThreshold",
                initialValue = weights(lower_index),
                retainState = true
        )

        state.newAdder(
                name = "upperThreshold",
                initialValue = weights(upper_index),
                retainState = true
        )
      }
      .edgeFilter(
              (edge, state) =>
                opLower(
                        edge.weight[T](),
                        state[T](
                                "lowerThreshold"
                        ).value
                ) &&
                  opUpper(
                          edge.weight[T](),
                          state[T](
                                  "upperThreshold"
                          ).value
                  ),
              pruneNodes = pruneNodes
      )
  }
}

object EdgeWeightQuantile {

  def apply[T: Numeric: ClassTag](
      lower: Float = 0.0f,
      upper: Float = 1.0f,
      weightString: String = "weight",
      lowerExclusive: Boolean = false,
      upperExclusive: Boolean = false,
      pruneNodes: Boolean = true
  ) =
    new EdgeWeightQuantile[T](
            lower,
            upper,
            weightString,
            lowerExclusive,
            upperExclusive,
            pruneNodes
    )
}
