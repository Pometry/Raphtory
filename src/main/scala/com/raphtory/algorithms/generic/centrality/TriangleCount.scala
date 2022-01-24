package com.raphtory.algorithms.generic.centrality

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row, Table}

/**
Description
  Returns the number of triangles.
  The triangle count algorithm counts the number of triangles
  (triplet of nodes which all pairwise share a link) that each
  vertex is a member of, from which additionally the global graph
  triangle count and clustering coefficient can be realised.

  The algorithm is similar to that of GraphX and fairly straightforward:
  1. Each vertex compiles a list of neighbours with ID strictly greater than its own,
  and sends this list to all neighbours as an array.
  2. Each vertex, starting from a triangle count of zero, looks at the lists of ID
  sets it has received, computes the intersection size of each list with its own list
  of neighbours, and adds this to the count.
  3. The total triangle count of the graph is calculated as the sum of the triangle
  counts of each vertex, divided by 3 (since each triangle is counted for each of the
  3 vertices involved).
  4. The clustering coefficient for each node is calculated as the triangle count for
  that node divided by the number of possible triangles for that node. The average
  clustering coefficient is the average of these over all the vertices.

Parameters
  path String : This takes the path of where the output should be written to

Returns
  ID (Long) : Vertex ID
  Triangle Count (Long) : Number of triangles

Notes
  Edges here are treated as undirected, so if the underlying network is directed here,
  'neighbours' refers to the union of in-neighbours and out-neighbours.
**/
class TriangleCount(path:String) extends GraphAlgorithm {

  override def apply(graph: GraphPerspective): GraphPerspective = {
    graph.step({
      vertex =>
        vertex.setState("triangles", 0)
        val neighbours = vertex.getAllNeighbours().toSet
        neighbours.foreach({
          nb =>
            vertex.messageVertex(nb, neighbours)
        })
    })
  }

  override def tabularise(graph: GraphPerspective): Table = {
    graph.select({
      vertex =>
        val neighbours = vertex.getAllNeighbours().toSet
        val queue = vertex.messageQueue[Set[Long]]
        var tri = 0
        queue.foreach(
          nbs =>
            tri += nbs.intersect(neighbours).size
        )
        vertex.setState("triangles", tri / 2)
        Row(vertex.name(), vertex.getState[Int]("triangles"))
    })
  }

  override def write(table: Table): Unit = {
    table.writeTo(path)
  }
}

object TriangleCount{
  def apply(path:String) = new TriangleCount(path:String)
}
