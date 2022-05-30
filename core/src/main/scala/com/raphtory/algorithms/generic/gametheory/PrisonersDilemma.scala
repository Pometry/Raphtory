package com.raphtory.algorithms.generic.gametheory

import com.raphtory.algorithms.generic.NodeList
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table
import com.raphtory.graph.visitor.Vertex

import scala.util.Random
import scala.collection.mutable
import scala.collection.mutable.Queue

/**
  * {s}`PrisonersDilemma(proportionCoop: Float = 0.5f, benefit: Float, cost: Float = 1.0f, noGames: Int = 100)`
  *   : run iterative game of prisoners dilemma
  *
  *   An iterative game of Prisoners' dilemma is played on the network. In each round, vertices simultaneously play a round
  *   of Prisoners' dilemma with their neighbours, getting a payoff according to whether each player chose to cooperate (C)
  *   or defect(D). After the game is played, a vertex adopts its strategy by choosing the highest scoring strategy in its
  *   direct neighbourhood. This carries on until either a pre-defined number of games is played or if none of the vertices
  *   change their strategy.
  *
  * ## Parameters
  *
  *   {s}`proportionCoop: Float=0.5f`
  *    : Proportion of vertices that start out as cooperators (uniformly sampled). Alternatively, vertices
  *     starting with a status "cooperator" set to 0 begin as cooperators.
  *
  *   {s}`benefit: Float`
  *    : Benefit parameter for prisoners' dilemma game. See [1]
  *
  *   {s}`cost: Float`
  *    : Cost parameter for prisoners' dilemma game. See [1]
  *
  *   {s}`noGames`
  *    : Maximum number of games to be played.
  *
  *   {s}`seed`
  *    : optional seed for testing purposes.
  *
  * ## States
  *
  *   {s}`cooperator: Int`
  *    : The latest status: cooperator (0), or defector (1), of the vertex at that iteration.
  *
  *   {s}`cooperationHistory: mutable.Queue[Int]`
  *    : The full history of a vertex' cooperation status for the games played.
  *
  * ## Returns
  *
  *   | vertex name       | cooperation history                 |
  *   | ----------------- | ----------------------------------- |
  *   | {s}`name: String` | {s}`cooperationHistory: Queue[Int]` |
  *
  * ## References
  *
  *   [1] Nowak, M. A., & May, R. M. (1992). Evolutionary games and spatial chaos. Nature, 359(6398), 826-829.
  *
  * ```{seealso}
  * [](com.raphtory.algorithms.generic.dynamic.WattsCascade)
  * ```
  */

class PrisonersDilemma(
    proportionCoop: Float = 0.5f,
    benefit: Float,
    cost: Float = 1.0f,
    noGames: Int = 100,
    seed: Int = -1
) extends NodeList(Seq("cooperationHistory")) {

  private val rnd = if (seed == -1) new Random() else new Random(seed)

  final private val COOPERATOR = 0
  final private val DEFECTOR   = 1

  final private val PLAYSTEP   = 0
  final private val UPDATESTEP = 1

  // Payoff Matrix [(b-c, b-c) , (-c,b), (b,-c), (0,0)]
  override def apply[G <: GraphPerspective[G]](graph: G): G = {

    graph.step { vertex =>
      vertex.getOrSetState[Int]("cooperator", if (rnd.nextFloat() < proportionCoop) 0 else 1)
      val cooperationStatus = vertex.getState[Int]("cooperator")
      vertex.setState("cooperationHistory", Queue(cooperationStatus))
      vertex.messageAllNeighbours(cooperationStatus)
      vertex.setState("step", PLAYSTEP)
    }

    graph.iterate(
            { vertex =>
              val stepType = vertex.getState[Int]("step")
              stepType match {
                case PLAYSTEP   => playStep(vertex, benefit, cost)
                case UPDATESTEP => updateStep(vertex)
              }
            },
            executeMessagedOnly = true,
            iterations = noGames
    )
  }

  def playStep(vertex: Vertex, benefit: Float, cost: Float): Unit = {
    val neighbourStatuses = vertex.messageQueue[Int]
    val myStatus          = vertex.getState[Int]("cooperator")
    val payoff            = neighbourStatuses
      .map(neighStatus => (myStatus, neighStatus))
      .map {
        case (COOPERATOR, COOPERATOR) => benefit - cost
        case (COOPERATOR, DEFECTOR)   => -cost
        case (DEFECTOR, COOPERATOR)   => benefit
        case (DEFECTOR, DEFECTOR)     => 0.0f
      }
      .sum
    vertex.messageAllNeighbours((myStatus, payoff))
    vertex.messageSelf((myStatus, payoff))
    vertex.setState("step", UPDATESTEP)
  }

  def updateStep(vertex: Vertex): Unit = {
    val neighbourStatuses = vertex.messageQueue[(Int, Float)]
    val currentStatus     = vertex.getState[Int]("cooperator")
    val newStatus         = neighbourStatuses.maxBy { case (status, score) => score }._1

    vertex.setState("cooperator", newStatus)
    val history = vertex.getState[Queue[Int]]("cooperationHistory")
    history += newStatus

    if (newStatus == currentStatus)
      vertex.voteToHalt()
    vertex.messageAllNeighbours(newStatus)
    vertex.setState("step", PLAYSTEP)
  }

  override def tabularise[G <: GraphPerspective[G]](graph: G): Table =
    graph.select { vertex =>
      val history = vertex.getState[mutable.Queue[Int]]("cooperationHistory")
      Row(vertex.name(), "[" + history.mkString(" ") + "]")
    }
}

object PrisonersDilemma {

  def apply(
      proportionCoop: Float = 0.5f,
      benefit: Float,
      cost: Float,
      noGames: Int = 100,
      seed: Int = -1
  ) =
    new PrisonersDilemma(proportionCoop, benefit, cost, noGames, seed)
}
