package com.raphtory.algorithms.generic.centrality

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row, Table}

/**
Description
  The degree of a node in an undirected networks counts the number of neighbours that
  node has. Its weighted degree counts the number of interactions each node has. Finally
  the edge weight counts the number of interactions which have happened across an edge.

Parameters
  path (String) : The path where the output will be written

Returns
  ID (Long) : Vertex ID
  indegree (Long) : The indegree of the node
  outdegree (Long) : The outdegree of the node
  total degree (Long) : The total degree
**/
class Degree(path:String) extends GraphAlgorithm{
  override def tabularise(graph: GraphPerspective): Table = {
    graph.select({
      vertex =>
        val inDegree = vertex.getInNeighbours().size
        val outDegree = vertex.getOutNeighbours().size
        val totalDegree = vertex.getAllNeighbours().size
        Row(vertex.name(), inDegree, outDegree, totalDegree)
    })
  }

  override def write(table: Table): Unit = {
    table.writeTo(path)
  }
}

object Degree{
  def apply(path:String) = new Degree(path)
}