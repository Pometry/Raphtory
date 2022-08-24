package com.raphtory.algorithms.temporal

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table
import com.raphtory.internals.communication.SchemaProviderInstances._

/**
  * {s}`Descendants(seed:String, time:Long, delta:Long=Long.MaxValue, directed:Boolean=true)`
  *  : find all descendants of a vertex at a given time point
  *
  * The descendants of a seed vertex are defined as those vertices which can be reached from the seed vertex
  * via a temporal path (in a temporal path the time of the next edge is always later than the time of the previous edge)
  * starting after the specified time.
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
  *    : The maximum timespan for the temporal path. This is currently exclusive of the newest time
  *       i.e. if looking forward a minute it will not include events that happen exactly 1 minute into the future.
  *
  *  {s}`directed: Boolean = true`
  *    : whether to treat the network as directed
  *
  *  {s}`strict: Boolean = true`
  *    : Whether firstActivityAfter is strict in its following of paths that happen exactly at the given time. True will not follow, False will.
  *
  * ## States
  *
  *  {s}`descendant: Boolean`
  *    : flag indicating that the vertex is a descendant of {s}`seed`
  *
  * ## Returns
  *
  *  | vertex name       | is descendant of seed?   |
  *  | ----------------- | ---------------------- |
  *  | {s}`name: String` | {s}`descendant: Boolean` |
  */
class Descendants(
    seed: String,
    time: Long,
    delta: Long = Long.MaxValue,
    directed: Boolean = true,
    strict: Boolean = true
) extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .step { vertex =>
        if (vertex.name() == seed) {
          (if (directed) vertex.outEdges else vertex.edges)
            .foreach(e =>
              e.firstActivityAfter(time, strict) match {
                case Some(event) =>
                  if (event.time < time + delta) vertex.messageVertex(e.ID, event.time)
                case None        =>
              }
            )
          vertex.setState("descendant", false)
        }
      }
      .iterate(
              { vertex =>
                val earliestTime = vertex.messageQueue[Long].min
                vertex.setState("descendant", true)
                (if (directed) vertex.outEdges else vertex.edges)
                  .foreach(e =>
                    e.firstActivityAfter(time, strict) match {
                      case Some(event) =>
                        if (event.time < time + delta)
                          vertex.messageVertex(e.ID, event.time)
                      case None        =>
                    }
                  )
              },
              executeMessagedOnly = true,
              iterations = 100
      )

  override def tabularise(graph: GraphPerspective): Table =
    graph.select(vertex => Row(vertex.name(), vertex.getStateOrElse[Boolean]("descendant", false)))
}

object Descendants {

  def apply(
      seed: String,
      time: Long,
      delta: Long = Long.MaxValue,
      directed: Boolean = true,
      strict: Boolean = true
  ) =
    new Descendants(seed, time, delta, directed, strict)
}
