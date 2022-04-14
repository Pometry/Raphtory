package com.raphtory.algorithms.temporal

import com.raphtory.algorithms.api.GraphAlgorithm
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table

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
  *    : The maximum timespan for the temporal path
  *
  *  {s}`directed: Boolean = true`
  *    : whether to treat the network as directed
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
class Descendants(seed: String, time: Long, delta: Long = Long.MaxValue, directed: Boolean = true)
        extends GraphAlgorithm {

  override def apply(graph: GraphPerspective): GraphPerspective =
    graph
      .step { vertex =>
        if (vertex.name() == seed) {
          val edges = (if (directed) vertex.getOutEdges() else vertex.getEdges())
            .filter(e => e.latestActivity().time > time)
            .filter(e =>
              e.firstActivityAfter(time) match {
                case Some(event) => event.time < time + delta
                case None        => false
              }
            )
          edges.foreach(e =>
            e.firstActivityAfter(time) match {
              case Some(event) => vertex.messageVertex(e.ID(), event.time)
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
                val outEdges     = (if (directed) vertex.getOutEdges() else vertex.getEdges())
                  .filter(e => e.latestActivity().time > earliestTime)
                  .filter(e =>
                    e.firstActivityAfter(time) match {
                      case Some(event) => event.time < time + delta
                      case None        => false
                    }
                  )
                outEdges.foreach(e =>
                  e.firstActivityAfter(time) match {
                    case Some(event) => vertex.messageVertex(e.ID(), event.time)
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

  def apply(seed: String, time: Long, delta: Long = Long.MaxValue, directed: Boolean = true) =
    new Descendants(seed, time, delta, directed)
}
