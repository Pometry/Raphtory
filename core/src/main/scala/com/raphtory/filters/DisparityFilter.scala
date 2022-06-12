package com.raphtory.filters

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
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
  *  Proceedings of the National Academy of Sciences 106.16 (2009): 6483-6488. Note that this implementation is aimed at undirected networks.
  *
  * ## Parameters
  *
  *  {s}`alpha: Double = 0.05`
  *  : Significance level to use. A smaller {s}`alpha` value means more edges will be removed
  *
  *  {s}`weightString: String = "weight"`
  *  : String name of the property/state, defaulting to "weight". As with other weighted algorithms in Raphtory, if no weight property
  *  is there but multi-edges are present, the number of occurrences of each edge is treated as the weight.
  *
  * ```{seealso}
  * [](com.raphtory.filters.EdgeQuantileFilter)
  * ```
  */

class DisparityFilter[T: Numeric: Bounded: ClassTag](
    alpha: Double = 0.05,
    weightProperty: String = "weight"
) extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .step { vertex =>
        // out neighbours means no need to send double messages
        vertex.messageOutNeighbours(
                vertex.ID,
                vertex.degree,
                vertex.weightedTotalDegree[T](weightProperty = weightProperty)
        )
      }
      .step { vertex =>
        val messages  = vertex.messageQueue[(vertex.IDType, Int, T)]
        val degreeMap = messages.groupBy(_._1).mapValues(_.head._2)
        val weightMap = messages.groupBy(_._1).mapValues(_.head._3)

        val k1 = vertex.degree
        val s1 = vertex.weightedTotalDegree[T](weightProperty = weightProperty).toDouble
        vertex.getInEdges().foreach { edge =>
          val k2  = degreeMap(edge.src)
          val s2  = weightMap(edge.src).toDouble
          val wgt = edge.weight[T](weightProperty = weightProperty)

          val (pij, pji) = (wgt.toDouble / s1, wgt.toDouble / s2)
          val (aij, aji) = (Math.pow(1.0 - pij, k1 - 1), Math.pow(1.0 - pji, k2 - 1))
          if (aij < alpha || aji < alpha)
            edge.remove()
        }
      }

}

object DisparityFilter {

  def apply[T: Numeric: Bounded: ClassTag](
      alpha: Double = 0.05,
      weightProperty: String = "weight"
  ) = new DisparityFilter[T](alpha, weightProperty)
}
