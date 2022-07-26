package com.raphtory.examples.bots.analysis

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

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
//out = retweet other person, in = being retweeted
  class Influence() extends Generic {
    override def apply(graph: GraphPerspective): graph.Graph =
      graph
        .step{
          (vertex,_) =>
            vertex.setState("Influence", 0)
        }
        .setGlobalState(state => state.newAdder[Float]("Influence", retainState = true))
        .step{
          (vertex,_)=>
            var  infIN = 0.0F
            var infOut = 0.0F
            var infInN = 0.0F
            var infInNTemp=0.0F
            var tmpDegree=0.0F

            vertex.getInEdges().foreach(
              edge => infIN += edge.getPropertyOrElse("sentiment","0.5F").toFloat
            )
            vertex.getOutEdges().foreach(
              edge => infOut += edge.getPropertyOrElse("sentiment","0.5F").toFloat
                )

            vertex.getInNeighbours().foreach(
              n => vertex.getInEdges().foreach(edge => infInNTemp += (edge.getPropertyOrElse("sentiment","0.5F").toFloat/vertex.inDegree)))

            if (vertex.outDegree !=0){
              infOut = infOut/vertex.outDegree
            }
            if (vertex.inDegree !=0) {
              infIN = infIN / vertex.inDegree
            }
            val totInf = ((infIN-infOut))
            vertex.setState("Influence", totInf)
        }
    override def tabularise(graph: GraphPerspective): Table =
      graph.select(vertex => Row(vertex.name(), vertex.getStateOrElse[Boolean]("Influence", false)))
      }

    object Influence {
      def apply() = new Influence()
    }

