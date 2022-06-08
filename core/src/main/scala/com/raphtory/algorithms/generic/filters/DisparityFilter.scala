package com.raphtory.algorithms.generic.filters

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.utils.Bounded

import scala.reflect.ClassTag

import scala.language.implicitConversions
import scala.math.Numeric.Implicits.infixNumericOps

class DisparityFilter[T: Numeric : Bounded: ClassTag](alpha: Float=0.05f, weightProperty:String) extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph = {
    graph.step{
      vertex =>
        // out neighbours means no need to send double messages
        vertex.messageOutNeighbours(vertex.ID, vertex.degree, vertex.weightedTotalDegree[T](weightProperty=weightProperty))
    }
      .step{
        vertex =>
          val messages = vertex.messageQueue[(vertex.IDType, Int, T)]
          val degreeMap = messages.groupBy(_._1).mapValues(_.head._2)
          val weightMap = messages.groupBy(_._1).mapValues(_.head._3)

          val k1 = vertex.degree
          val s1 = vertex.weightedTotalDegree[T](weightProperty = weightProperty)
          vertex.getInEdges().foreach{
            edge =>
              val k2 = degreeMap(edge.src)
              val s2 = weightMap(edge.src)
              val wgt = edge.weight[T](weightProperty = weightProperty)

              val (pij, pji) = (wgt.toDouble/s1, wgt.toDouble/s2)
              val (aij, aji) = (Math.pow(1.0-pij, k1 -1), Math.pow(1.0-pji,k2-1))
              if (aij < alpha || aji < alpha)
                edge.remove()
          }
      }
  }

}

object DisparityFilter {
  def apply[T: Numeric: Bounded: ClassTag](alpha: Float=0.05f, weightProperty:String) = new DisparityFilter[T](alpha,weightProperty)
}
