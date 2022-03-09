package com.raphtory.algorithms.generic

import com.raphtory.algorithms.api.GraphAlgorithm
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table

/**
  * {s}`ConnectedComponents()`
  * : Identify connected components
  *
  * A connected component of an undirected graph is a set of vertices all of which
  * are connected by paths to each other. This algorithm identifies the connected component
  * each vertex belongs to.
  *
  * ```{note}
  * Edges here are treated as undirected, a future feature may be to calculate
  * weakly/strongly connected components for directed networks.
  * ```
  *
  * ## States
  *
  * {s}`cclabel: Long`
  * : Label of connected component the vertex belongs to (minimum vertex ID in component)
  *
  * ## Returns
  *
  * | vertex name       | component label    |
  * | ----------------- | ------------------ |
  * | {s}`name: String` | {s}`cclabel: Long` |
  *
  * ## Implementation
  *
  * The algorithm is similar to that of GraphX and fairly straightforward:
  *
  *  1. Each node is numbered by its ID, and takes this as an initial connected components label.
  *
  *  2. Each node forwards its own label to each of its neighbours.
  *
  *  3. Having received a list of labels from neighbouring nodes, each node relabels itself with the smallest
  *     label it has received (or stays the same if its starting label is smaller than any received).
  *
  *  4. The algorithm iterates over steps 2 and 3 until no nodes change their label within an iteration.
  */
class ConnectedComponents() extends NodeList(Seq("cclabel")) {

  override def apply(graph: GraphPerspective): GraphPerspective =
//    TODO: Uncommenting any of these lines fixes the twitter test
//    println(s"Finding components for graph with ${graph.nodeCount()} nodes")
//    println("")
//    val n = graph.nodeCount()
//    val x = 0
    graph
      .step { vertex =>
        vertex.setState("cclabel", vertex.ID)
        vertex.messageAllNeighbours(vertex.ID)
      }
      .iterate(
              { vertex =>
                val label = vertex.messageQueue[Long].min
                if (label < vertex.getState[Long]("cclabel")) {
                  vertex.setState("cclabel", label)
                  vertex.messageAllNeighbours(label)
                }
                else
                  vertex.voteToHalt()
              },
              iterations = 100,
              executeMessagedOnly = true
      )
}

object ConnectedComponents {
  def apply() = new ConnectedComponents()
}
