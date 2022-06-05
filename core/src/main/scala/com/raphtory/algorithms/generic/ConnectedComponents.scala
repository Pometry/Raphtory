package com.raphtory.algorithms.generic

import com.raphtory.api.analysis.graphview.GraphPerspective

import scala.math.Ordering.Implicits._

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
object ConnectedComponents extends NodeList(Seq("cclabel")) {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .step { vertex =>
        vertex.setState("cclabel", vertex.ID)
        vertex.messageAllNeighbours(vertex.ID)
      }
      .iterate(
              { vertex =>
                import vertex.IDOrdering
                val label = vertex.messageQueue[vertex.IDType].min
                if (label < vertex.getState[vertex.IDType]("cclabel")) {
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
