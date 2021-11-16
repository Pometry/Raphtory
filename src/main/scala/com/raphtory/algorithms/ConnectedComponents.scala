package com.raphtory.algorithms

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}


/**
Description
  A connected component of an undirected graph is a set of vertices all of which
  are connected by paths to each other. This algorithm calculates the number of
  connected components in the graph and the size of each, and returns some statisics
  of these.

Parameters
  path String : This takes the path of where the output should be written to

Returns
  ID (Long) : This is the ID of the vertex
  Label (Long) : The value of the component it belongs to

Notes
  Edges here are treated as undirected, a future feature may be to calculate
  weakly/strongly connected components for directed networks.

**/
class ConnectedComponents(path:String) extends GraphAlgorithm{
  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .step({
        vertex =>
          vertex.setState("cclabel", vertex.ID)
          vertex.messageAllNeighbours(vertex.ID)
      })
      .iterate({
        vertex =>
          val label = vertex.messageQueue[Long].min
          if (label < vertex.getState[Long]("cclabel")) {
            vertex.setState("cclabel", label)
            vertex messageAllNeighbours label
          }
          else
            vertex.voteToHalt()
      }, 100,true)
      .select(vertex => Row(vertex.ID(),vertex.getState[Long]("cclabel")))
      .writeTo(path)
  }
}

object ConnectedComponents{
  def apply(path:String) = new ConnectedComponents(path)
}
