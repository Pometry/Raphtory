package com.raphtory.algorithms.temporal

import com.raphtory.core.algorithm.GraphAlgorithm
import com.raphtory.core.algorithm.GraphPerspective
import com.raphtory.core.algorithm.Row
import com.raphtory.core.algorithm.Table

/**
  * {s}`Ancestors(seed:String, time:Long, delta:Long=Long.MaxValue, directed:Boolean=true)`
  *  : find all ancestors of a vertex at a given time point
  *
  * The ancestors of a seed vertex are defined as those vertices which can reach the seed vertex via a temporal
  * path (in a temporal path the time of the next edge is always later than the time of the previous edge) by the
  * specified time.
  *
  * ## Parameters
  *
  *  {s}`seed: String`
  *    : The name of the target vertex
  *
  *  {s}`time: Long`
  *    : The time of interest
  *
  *  {s}`delta: Long = Long.MaxValue`
  *    : The maximum timespan for the temporal path
  *
  *  {s}`directed: Boolean = true`
  *    : whether to treat the network as directed
  *
  * ## States
  *
  *  {s}`ancestor: Boolean`
  *    : flag indicating that the vertex is an ancestor of {s}`seed`
  *
  * ## Returns
  *
  *  | vertex name       | is ancestor of seed?   |
  *  | ----------------- | ---------------------- |
  *  | {s}`name: String` | {s}`ancestor: Boolean` |
  */
class Ancestors(seed: String, time: Long, delta: Long = Long.MaxValue, directed: Boolean = true)
        extends GraphAlgorithm {

  override def apply(graph: GraphPerspective): GraphPerspective =
    graph
      .step { vertex =>
        if (vertex.name() == seed) {
          val edges = (if (directed) vertex.getInEdges() else vertex.getEdges())
            .filter(e => e.earliestActivity().time < time)
            .filter(e => e.lastActivityBefore(time).time > time - delta)
          edges.foreach(e => vertex.messageVertex(e.ID(), e.lastActivityBefore(time).time))
          vertex.setState("ancestor", false)
        }
      }
      .iterate(
              { vertex =>
                val latestTime = vertex.messageQueue[Long].max
                vertex.setState("ancestor", true)
                val inEdges    = (if (directed) vertex.getInEdges() else vertex.getEdges())
                  .filter(e => e.earliestActivity().time < latestTime)
                  .filter(e => e.lastActivityBefore(time).time > latestTime - delta)
                inEdges
                  .foreach(e => vertex.messageVertex(e.ID(), e.lastActivityBefore(latestTime).time))
              },
              executeMessagedOnly = true,
              iterations = 100
      )

  override def tabularise(graph: GraphPerspective): Table =
    graph.select(vertex => Row(vertex.name(), vertex.getStateOrElse[Boolean]("ancestor", false)))

}

object Ancestors {

  def apply(seed: String, time: Long, delta: Long = Long.MaxValue, directed: Boolean = true) =
    new Ancestors(seed, time, delta, directed)
}
