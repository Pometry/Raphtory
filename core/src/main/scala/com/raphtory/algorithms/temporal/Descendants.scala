package com.raphtory.algorithms.temporal

import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table
import com.raphtory.algorithms.api.algorithm.GenericAlgorithm

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
) extends GenericAlgorithm {

  override def apply[G <: GraphPerspective[G]](graph: G): G =
    graph
      .step { vertex =>
        if (vertex.name() == seed) {
          (if (directed) vertex.getOutEdges() else vertex.getEdges())
            .foreach(e =>
              e.firstActivityAfter(time, strict) match {
                case Some(event) =>
                  if (event.time < time + delta) vertex.messageVertex(e.ID(), event.time)
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
                (if (directed) vertex.getOutEdges() else vertex.getEdges())
                  .foreach(e =>
                    e.firstActivityAfter(time, strict) match {
                      case Some(event) =>
                        if (event.time < time + delta)
                          vertex.messageVertex(e.ID(), event.time)
                      case None        =>
                    }
                  )
              },
              executeMessagedOnly = true,
              iterations = 100
      )

  override def tabularise[G <: GraphPerspective[G]](graph: G): Table =
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
