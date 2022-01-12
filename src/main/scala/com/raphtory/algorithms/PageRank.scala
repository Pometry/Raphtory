package com.raphtory.algorithms

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row, Table}


/**
Description
  Page Rank algorithm ranks nodes depending on their connections to determine how important
  the node is. This assumes a node is more important if it receives more connections from others.
  Each vertex begins with an initial state. If it has any neighbours, it sends them a message
  which is the inital label / the number of neighbours.
  Each vertex, checks its messages and computes a new label based on: the total value of
  messages received and the damping factor. This new value is propogated to all outgoing neighbours.
  A vertex will stop propogating messages if its value becomes stagnant (i.e. has a change of less
  than 0.00001) This process is repeated for a number of iterate step times. Most algorithms should
  converge after approx. 20 iterations.

Parameters
  dampingFactor (Double) : Probability that a node will be randomly selected by a user traversing the graph, defaults to 0.85.
  iterateSteps (Int) : Number of times for the algorithm to run.
  output (String) : The path where the output will be saved. If not specified, defaults to /tmp/PageRank

Returns
  ID (Long) : Vertex ID
  Page Rank (Double) : Rank of the node
**/
class PageRank(dampingFactor:Double = 0.85, iterateSteps:Int = 100, output:String = "/tmp/PageRank") extends  GraphAlgorithm {

  override def graphStage(graph: GraphPerspective): GraphPerspective = {
    graph.step({
      vertex =>
        val initLabel = 1.0
        vertex.setState("prlabel", initLabel)
        val outDegree = vertex.getOutNeighbours().size
        if (outDegree > 0.0)
          vertex.messageAllOutgoingNeighbors(initLabel / outDegree)
    }).
      iterate({ vertex =>
        val vname = vertex.getPropertyOrElse("name", vertex.ID().toString) // for logging purposes
        val currentLabel = vertex.getState[Double]("prlabel")

        val queue = vertex.messageQueue[Double]
        val newLabel = (1 - dampingFactor) + dampingFactor * queue.sum
        vertex.setState("prlabel", newLabel)

        val outDegree = vertex.getOutNeighbours().size
        if (outDegree > 0) {
          vertex.messageAllOutgoingNeighbors(newLabel / outDegree)
        }

        if (Math.abs(newLabel - currentLabel) / currentLabel < 0.00001) {
          vertex.voteToHalt()
        }
      }, iterateSteps, false) // make iterate act on all vertices, not just messaged ones
  }

  override def tableStage(graph: GraphPerspective): Table = {
    graph.select({
      vertex =>
        Row(
          vertex.getPropertyOrElse("name", vertex.ID()),
          vertex.getStateOrElse("prlabel", -1)
        )
    })
  }

  override def write(table: Table): Unit = {
    table.writeTo(output)
  }
}

object PageRank{
  def apply(dampingFactor:Double = 0.85, iterateSteps:Int = 100, output:String = "/tmp/PageRank") =
    new PageRank(dampingFactor, iterateSteps, output)
}