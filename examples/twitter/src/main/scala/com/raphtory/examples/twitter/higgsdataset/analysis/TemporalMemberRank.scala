package com.raphtory.examples.twitter.higgsdataset.analysis

import com.raphtory.api.analysis.algorithm.GenericReduction
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.graphview.ReducedGraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

/**
  * Description
  * This algorithm takes vertices with big differences in their raw scores and MemberRank scores
  * and checks the in edge creations over time.
  */
class TemporalMemberRank() extends GenericReduction {

  case class NeighbourAndTime[T](id: T, time: Long)

  override def apply(graph: GraphPerspective): graph.ReducedGraph =
    graph.reducedView.step { vertex =>
      // The original scores that someone received by their peers

      val negativeRaw = Math.abs(vertex.getState[Double]("negativeRawScore"))
      val positiveRaw = Math.abs(vertex.getState[Double]("positiveRawScore"))

      /**
        *  Our model score.
        *  If the person is influential, they will bump the value to something high
        *  if the person is non-influential (maybe bot?) the value will be small
        */

      val negativeNew = Math.abs(vertex.getState[Double]("negativeNewScore"))
      val positiveNew = Math.abs(vertex.getState[Double]("positiveNewScore"))
      //is the raw value significantly different to the new score (factor = 2 but this can be changed)
      val difference: Boolean = (positiveRaw > (positiveNew * 2))
      /**
        *  if difference between raw and new is greater than zero
        *  return list of times of in edge creation for each vertex
        */

      val times: Seq[NeighbourAndTime[vertex.IDType]] = vertex.explodeInEdges().collect {
        case edge if difference => NeighbourAndTime(edge.src, edge.timestamp)
      }


      vertex.setState("times", times)
    }

  // Tabularises results to Row(Raphtory Timestamp, suspected bot ID, ID of retweeted user, timestamp of retweet)
  override def tabularise(graph: ReducedGraphPerspective): Table =
    graph
      .select { vertex =>
        Row(
                vertex.ID,
                vertex.getState("times")
        )
      }
      .filter { row =>
        val times = row.getAs[Seq[NeighbourAndTime[_]]](1)
        times.nonEmpty
      }
      .explode { row =>
        val rowId = row.getLong(0)
        val times = row.getAs[Seq[NeighbourAndTime[_]]](1)
        times.map { neighbourAndTime =>
          Row(rowId, neighbourAndTime.id, neighbourAndTime.time)
        }.toList
      }

}

object TemporalMemberRank {
  def apply() = new TemporalMemberRank()
}
