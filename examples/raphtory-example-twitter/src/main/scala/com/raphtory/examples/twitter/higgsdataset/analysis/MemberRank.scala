package com.raphtory.examples.twitter.higgsdataset.analysis

import com.raphtory.api.algorithm.Generic
import com.raphtory.api.graphview.GraphPerspective
import com.raphtory.api.table.Row
import com.raphtory.api.table.Table
import com.raphtory.api.visitor.Edge

/**
  * Description
  * This algorithm takes the Page Rank Score from the vertices neighbour's and multiplies
  * this by the vertices ranking from the original raw dataset (the vertex in degree). If the vertex gets a high MemberRank score,
  * this means that other people that have high Page Rank values have rated them highly. This should dampen
  * the effects of bots further.
  */

class MemberRank() extends Generic {

  case class Score(negativeScore: Double = 0.0, positiveScore: Double = 0.0) {

    def +(that: Score): Score = {
      val negative = this.negativeScore + that.negativeScore
      val positive = this.positiveScore + that.positiveScore

      Score(negative, positive)
    }
  }

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .step { vertex =>
        //For this vertex get our Page Rank value
        val prValue = vertex.getState[Double]("prlabel")
        //Message all of our outgoing neighbours with our ID + Page Rank value
        vertex.messageOutNeighbours((vertex.ID(), prValue))
      }
      .step { vertex =>
        //Get the Vertex ID and Page Rank (Long,Double) messages from our neighbours
        val queue: Seq[(vertex.IDType, Double)] = vertex.messageQueue[(vertex.IDType, Double)]

        //Get the vertex in degree
        val inDegree = vertex.getInNeighbours().size.toDouble

        //makes list of all the raw scores from data
        val rawScoreList: List[Score] = vertex.getInEdges().map {
          case edge: Edge =>
            if (inDegree < 0) Score(negativeScore = inDegree)
            else Score(positiveScore = inDegree)
          case _          => Score()
        }

        //Take output of val rawScoreList, sum all the positive scores and position on left of tuple, sum all negative scores and position on right of tuple. Does this for each vertex.
        val totalRaw: Option[Score] = rawScoreList
          .reduceLeftOption((scoreLeft, scoreRight) => scoreLeft + scoreRight)

        //Extract negative and positive scores from totalRaw tuple.
        val negativeRawScore = totalRaw.fold(0.0)(_.negativeScore)
        val positiveRawScore = totalRaw.fold(0.0)(_.positiveScore)

        val newScore: Seq[Score] = queue.map {
          case (neighbourID, prScore) =>
            vertex.getInEdge(neighbourID) match {
              case Some(edge) =>
                //Multiplies page rank score of neighbour (how popular it is) to its own score
                if (inDegree < 0) Score(negativeScore = inDegree * prScore)
                else Score(positiveScore = inDegree * prScore)

              case None       => Score()
            }
        }

        //Take output of val newScore, sum all the positive scores and position on left of tuple, sum all negative scores and position on right of tuple. Does this for each vertex.
        val totalScore: Option[Score] = newScore
          .reduceOption((scoreLeft, scoreRight) => scoreLeft + scoreRight)

        //Extract negative and positive scores from totalScore tuple.
        val negativeNewScore = totalScore.fold(0.0)(_.negativeScore)
        val positiveNewScore = totalScore.fold(0.0)(_.positiveScore)

        //set's vertex state with these scores
        vertex.setState("negativeRawScore", negativeRawScore)
        vertex.setState("negativeNewScore", negativeNewScore)
        vertex.setState("positiveRawScore", positiveRawScore)
        vertex.setState("positiveNewScore", positiveNewScore)
      }

  /**
    * Reports the following:
    *   1) Vertex ID
    *   2) Page Rank Score
    *   3) Original Negative Score
    *   4) Original Positive Score
    *   5) New Negative Score
    *   6) New Positive Score
    */
  override def tabularise(graph: GraphPerspective): Table =
    graph.select { vertex =>
      Row(
              vertex.getPropertyOrElse("name", vertex.ID()),
              //a vertices page rank score
              vertex.getStateOrElse("prlabel", -1),
              //gets vertex states and tabularises
              vertex.getStateOrElse("negativeRawScore", 0.0),
              vertex.getStateOrElse("positiveRawScore", 0.0),
              vertex.getStateOrElse("negativeNewScore", 0.0),
              vertex.getStateOrElse("positiveNewScore", 0.0)
      )
    }
}

object MemberRank {
  def apply() = new MemberRank()
}
