package com.raphtory.algorithms.generic.motif

import com.raphtory.algorithms.generic.{KCore, NodeList, motif}
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.internals.communication.SchemaProviderInstances._

/**
  * {s}`LocalTriangleCount()`
  *  : Returns the number of triangles.
  *
  * The triangle count algorithm counts the number of triangles
  * (triplet of nodes which all pairwise share a link) that each
  * vertex is a member of, from which additionally the global graph
  * triangle count and clustering coefficient can be realised.
  *
  * The algorithm is similar to that of GraphX and fairly straightforward:
  * 1. Each vertex compiles a list of neighbours with ID strictly greater than its own,
  * and sends this list to all neighbours as an array.
  * 2. Each vertex, starting from a triangle count of zero, looks at the lists of ID
  * sets it has received, computes the intersection size of each list with its own list
  * of neighbours, and adds this to the count.
  * 3. The total triangle count of the graph is calculated as the sum of the triangle
  * counts of each vertex, divided by 3 (since each triangle is counted for each of the
  * 3 vertices involved).
  * 4. The clustering coefficient for each node is calculated as the triangle count for
  * that node divided by the number of possible triangles for that node. The average
  * clustering coefficient is the average of these over all the vertices.
  *
  * ## States
  *
  *  {s}`triangleCount: Long`
  *    : Number of triangles a vertex is part of
  *
  * ## Returns
  *
  *  | vertex name       | number of triangles      |
  *  | ----------------- | ------------------------ |
  *  | {s}`name: String` | {s}`triangleCount: Long` |
  *
  * ```{note}
  *  Edges here are treated as undirected, so if the underlying network is directed here,
  * 'neighbours' refers to the union of in-neighbours and out-neighbours.
  * ``
  */
class LocalTriangleCount extends NodeList(Seq("triangleCount")) {

  override def apply(graph: GraphPerspective): graph.Graph =
    KCore(2).apply(graph)
      .clearMessages()
      // Filter step 1: tell neighbours you are in the filter
      .step{v =>
        v.setState("triangleCount", 0)
        if (v.getState[Int]("effectiveDegree")>=2) {
          v.messageAllNeighbours(v.ID)
        }
      }
      // Filter step 2: send neigbours in filter to other neighbours in filter
      .step{v =>
        if (v.getState[Int]("effectiveDegree")>=2) {
          val neighbours = v.messageQueue[v.IDType].toSet
          v.setState("effNeighbours",neighbours)
          neighbours.foreach(nb => v.messageVertex(nb, neighbours))
        }
      }
//      .step { (vertex, _) =>
//        vertex.setState("triangleCount", 0)
//        val neighbours = vertex.neighbours.toSet
//        neighbours.foreach(nb => vertex.messageVertex(nb, neighbours))
//      }
      .step { v =>
        if (v.getState[Int]("effectiveDegree")>=2) {
          val neighbours = v.getState[Set[v.IDType]]("effNeighbours")
          val queue = v.messageQueue[Set[v.IDType]]
          var tri = 0
          queue.foreach(nbs => tri += nbs.intersect(neighbours).size)
          v.setState("triangleCount", tri / 2)
        }
        v.clearState("effectiveDegree")
        v.clearState("effNeighbours")
      }
}

// THIS IS USED FOR PYTHON
object LocalTriangleCount {
  def apply() = new LocalTriangleCount()

  def apply(graph: GraphPerspective) = new LocalTriangleCount().apply(graph)
}
