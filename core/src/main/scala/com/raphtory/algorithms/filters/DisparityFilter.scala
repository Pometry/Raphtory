package com.raphtory.algorithms.filters

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.internals.communication.SchemaProviderInstances._
import com.raphtory.utils.Bounded

import scala.reflect.ClassTag
import scala.language.implicitConversions
import scala.math.Numeric.Implicits.infixNumericOps

/**
  * {s}`DisparityFilter(alpha: Double=0.05, weightProperty:String="weight")`
  * : Filtered view of a weighted graph based on Edge Disparity.
  *
  *  This creates a filtered view of a weighted graph where only "statistically significant" edges remain. For a description of this method,
  *  please refer to: Serrano, M. Ángeles, Marián Boguná, and Alessandro Vespignani. "Extracting the multiscale backbone of complex weighted networks."
  *  Proceedings of the National Academy of Sciences 106.16 (2009): 6483-6488. Note that this implementation is aimed for directed networks.
  *
  * ## Parameters
  *
  *  {s}`alpha: Double = 0.05`
  *  : Significance level to use. Edges with a p-values larger than {s}`alpha` are rmoved. A smaller {s}`alpha` value means more edges will be removed.
  *
  *  {s}`weightString: String = "weight"`
  *  : String name of the property/state, defaulting to "weight". As with other weighted algorithms in Raphtory, if no weight property
  *  is there but multi-edges are present, the number of occurrences of each edge is treated as the weight.
  *
  * ```{seealso}
  * [](com.raphtory.algorithms.filters.EdgeQuantileFilter)
  * ```
  */

// problem
class DisparityFilter[T: Numeric: Bounded: ClassTag](
    alpha: Double = 0.05,
    weightProperty: String = "weight"
) extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .step { vertex =>
        val s = vertex.weightedOutDegree[T](weightProperty = weightProperty).toDouble
        val k = vertex.outDegree

        vertex.outEdges.foreach { edge =>
          val w   = edge.weight[T](weightProperty = weightProperty).toDouble
          val pij = w / s

          val p_value = Math.pow(1.0 - pij, k - 1)
          if (p_value > alpha)
            edge.remove()
        }
      }

}

class DisparityPValues[T: Numeric: Bounded: ClassTag](weightProperty: String = "weight", pvalueLabel: String = "pvalue")
        extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .step { vertex =>
        val s = vertex.weightedOutDegree[T](weightProperty = weightProperty).toDouble
        val k = vertex.outDegree

        vertex.outEdges.foreach { edge =>
          val w   = edge.weight[T](weightProperty = weightProperty).toDouble
          val pij = w / s

          val p_value = Math.pow(1.0 - pij, k - 1)

          edge.setState(pvalueLabel, p_value)
        }
      }

}

object DisparityPValues {

  def apply[T: Numeric: Bounded: ClassTag](
      weightProperty: String = "weight"
  ) = new DisparityPValues[T](weightProperty)
}

object DisparityFilter {

  def apply[T: Numeric: Bounded: ClassTag](
      alpha: Double = 0.05,
      weightProperty: String = "weight"
  ) = new DisparityFilter[T](alpha, weightProperty)
}