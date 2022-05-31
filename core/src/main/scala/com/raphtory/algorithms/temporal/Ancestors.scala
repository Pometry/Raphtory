package com.raphtory.algorithms.temporal

import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table
import com.raphtory.algorithms.api.algorithm.GenericAlgorithm

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
  *    : The maximum timespan for the temporal path. This is currently exclusive of the oldest time
  *       i.e. if looking back a minute it will not include events that happen exactly 1 minute ago.
  *
  *  {s}`directed: Boolean = true`
  *    : whether to treat the network as directed
  *
  *  {s}`strict: Boolean = true`
  *    : Whether lastActivityBefore is strict in its following of paths that happen exactly at the given time. True will not follow, False will.
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
class Ancestors(
    seed: String,
    time: Long,
    delta: Long = Long.MaxValue,
    directed: Boolean = true,
    strict: Boolean = true
) extends GenericAlgorithm {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .step { vertex =>
        if (vertex.name() == seed) {
          (if (directed) vertex.getInEdges() else vertex.getEdges())
            .foreach(e =>
              e.lastActivityBefore(time, strict) match {
                case Some(event) =>
                  if (event.time > time - delta)
                    vertex.messageVertex(e.ID(), event.time)
                case None        =>
              }
            )
          vertex.setState("ancestor", false)
        }
      }
      .iterate(
              { vertex =>
                val latestTime = vertex.messageQueue[Long].max
                vertex.setState("ancestor", true)
                (if (directed) vertex.getInEdges() else vertex.getEdges())
                  .foreach(e =>
                    e.lastActivityBefore(time, strict) match {
                      case Some(event) =>
                        if (event.time > time - delta)
                          vertex.messageVertex(e.ID(), event.time)
                      case None        =>
                    }
                  )
              },
              executeMessagedOnly = true,
              iterations = 100
      )

  override def tabularise(graph: GraphPerspective): Table =
    graph.select(vertex => Row(vertex.name(), vertex.getStateOrElse[Boolean]("ancestor", false)))

}

object Ancestors {

  def apply(
      seed: String,
      time: Long,
      delta: Long = Long.MaxValue,
      directed: Boolean = true,
      strict: Boolean = true
  ) =
    new Ancestors(seed, time, delta, directed, strict)
}
