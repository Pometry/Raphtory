package com.raphtory.algorithms.generic.distance

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table
import com.raphtory.utils.Bounded
import com.raphtory.utils.Bounded._
import math.Numeric.Implicits._
import math.Ordering.Implicits._

/**
 * {s}`ShortestPathDistance(src_name: String, tgt_name: String, it: Int = 1000)`
 *  : compute the shortest path distance from vertex `src_name` to vertex `tgt_name`
 *
 * This algorithm returns the distance of the shortest path going from vertex `src_name` to vertex `tgt_name`
 * after at most `it` interations of the shortest path updates - default is `it=1000`.
 *
 * ## Parameters
 *
 *  {s}`src_name: String`
 *    : the source vertex, where the path must start from.
 *
 *  {s}`tgt_name: String`
 *    : the target vertex, where the path must end to.
 *
 * ## States
 *
 *  {s}`DISTANCE: T`
 *    : Distance to source vertex
 *
 * ## Returns
 *
 *  |     source vertex     |      target vertex    |      distance    |
 *  | --------------------- | --------------------- | -----------------|
 *  | {s}`src_name: String` | {s}`tgt_name: String` | {s}`DISTANCE: T` |
 *
 * ```{seealso}
 * [](com.raphtory.algorithms.generic.dynamic.RandomWalk)
 * ```
 */

class ShortestPathDistance[T: Bounded: Numeric](src_name: String,
                                                tgt_name: String,
                                                it: Int = 1000) extends Generic {

  final val DISTANCE = "DISTANCE"

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .setGlobalState({
        graphState =>
          graphState.newMin[T]("minDistance")
      })
      .step ( { (vertex, graphState) =>
        if (vertex.name() == src_name) {
          vertex.setState(DISTANCE, 0)
          vertex.getOutEdges().foreach { edge => edge.send(edge.weight[T]())}
          graphState("minDistance") += MAX[T]
        }
        else {
          vertex.setState(DISTANCE, MAX[T])
        }
      }
      )
      .iterate( {
        (vertex, graphState) =>
          val candidate_distance = vertex.messageQueue[T].min
          if (candidate_distance <= vertex.getState[T](DISTANCE)) {
            vertex.setState(DISTANCE, candidate_distance)
            if (vertex.name() == tgt_name) {
              graphState("minDistance") += candidate_distance
            }
            else {
            vertex.getOutEdges().foreach {edge =>
              val dummy = candidate_distance + edge.weight[T]()
              if (dummy <= graphState("minDistance").value) {
                edge.send(dummy)
              }
            }
            }
          }
          else {
            vertex.voteToHalt()
          }
      },
        iterations = it,
        executeMessagedOnly = true
      )

  override def tabularise(graph: GraphPerspective): Table =
    graph
      .explodeSelect { vertex =>
        val name = vertex.name()
        if (name == tgt_name) {
          List(Row(src_name, name, vertex.getState[T](DISTANCE)))
        } else {
          List.empty[Row]
        }
      }
}