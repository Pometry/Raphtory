package com.raphtory.algorithms.generic.centrality

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table
import com.raphtory.internals.communication.SchemaProviderInstances._

/**
  * {s}`Assortativity()`
  *  : compute assortativity coefficient of networks
  *
  * Assortativity coefficient is the Pearson coefficient between pairs of linked nodes
  * the value is between [-1,1]
  * positive value indicate a correlation between nodes of similar degree, and vice versa.
  *
  *
  * ```{note}
  * The network is treated as undirected.
  *
  *
  * ## Returns
  * | vertex name       | Assortativity              |
  * | ----------------- | -------------------------- |
  * | {s}`name: String` | {s}`Assortativity: Double` |
  *
  *
  */
class Assortativity extends Generic {
  override def apply(graph: GraphPerspective): graph.Graph = {
    graph
      .setGlobalState{
        globalState =>
          globalState.newAccumulator("linkCounter", Map[String, Double](), retainState = true, MapMerge)
          globalState.newAccumulator("nodeDegrees", List[Int](), retainState = true, ListMerge)
          globalState.newAdder[Double]("sumOfDegrees", retainState = true)
      }
      .step{
        (vertex, globalState) =>
          vertex.messageAllNeighbours(vertex.degree)
          globalState("sumOfDegrees") += vertex.degree.toDouble
          globalState("nodeDegrees") += List(vertex.degree)
      }
      .step{
        (vertex, globalState) =>
          vertex.messageQueue[Int].foreach{
            degree =>
              globalState("linkCounter") +=
                (Map[String, Double](vertex.degree.toString + ',' + degree.toString -> 1.0))
          }
      }
  }

  def MapMerge(x: Map[String, Double], y: Map[String, Double]): Map[String, Double] = {
    x ++ y.map(t => t._1 -> (t._2 + x.getOrElse(t._1, 0.0)))
  }

  def ListMerge(x: List[Int], y: List[Int]): List[Int] = {
    if (y.nonEmpty) {
      if (x.contains(y.head)) x
      else x ++ y
    }
    else x
  }

  def calculation(njk: Map[String, Double], degrees: List[Int], edges: Double): Double = {
    var sigma_1 = 0.0
    var sigma_2 = 0.0
    var molecular_1 = 0.0
    var molecular_2 = 0.0
    val excessDegreeDistribution: collection.mutable.Map[Int, Double] = collection.mutable.Map[Int, Double]()
    var Pjk: Map[String, Double] = Map()
    degrees.foreach({
      k =>
        degrees.foreach({
          j =>
            Pjk = MapMerge(Pjk, Map[String, Double](k.toString + ',' + j.toString -> njk.getOrElse(k.toString + ',' + j.toString, 0.0) / edges))
        })
    })
    degrees.foreach({
      k =>
        degrees.foreach({
          j =>
            if (excessDegreeDistribution.contains(k)) {
              excessDegreeDistribution(k) = excessDegreeDistribution.getOrElse(k, 0.0) + Pjk.getOrElse(j.toString + ',' + k.toString, 0.0)
            }
            else {
              excessDegreeDistribution += (k -> Pjk.getOrElse(j.toString + ',' + k.toString, 0.0))
            }
        })
    })
    degrees.foreach({
      degreeK =>
        sigma_1 += math.pow(degreeK, 2) * excessDegreeDistribution.getOrElse(degreeK, 0.0)
        sigma_2 += degreeK * excessDegreeDistribution.getOrElse(degreeK, 0.0)
        degrees.foreach({
          degreeJ =>
            molecular_1 += degreeJ * degreeK * Pjk.getOrElse(degreeJ.toString + ',' + degreeK.toString, 0.0)
            molecular_2 += degreeJ * degreeK * excessDegreeDistribution.getOrElse(degreeJ, 0.0) * excessDegreeDistribution.getOrElse(degreeK, 0.0)
        })
    })
    sigma_2 = math.pow(sigma_2, 2)
    (molecular_1 - molecular_2) / ((sigma_1 - sigma_2))
  }

  override def tabularise(graph: GraphPerspective): Table = {
    graph.globalSelect(
      graphState =>
        Row("Assortativity = " + calculation(graphState("linkCounter").value, graphState("nodeDegrees").value, graphState("sumOfDegrees").value))
    )
  }
}

object Assortativity{
  def apply() = new Assortativity
}
