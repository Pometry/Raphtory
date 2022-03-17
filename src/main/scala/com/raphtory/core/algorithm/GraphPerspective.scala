package com.raphtory.core.algorithm

import com.raphtory.core.components.querymanager.QueryManagement
import com.raphtory.core.graph.visitor.Vertex

sealed trait GraphFunction                    extends QueryManagement
final case class Setup(f: GraphState => Unit) extends GraphFunction
final case class Step(f: (Vertex) => Unit)    extends GraphFunction

final case class StepWithGraph(
    f: (Vertex, GraphState) => Unit,
    graphState: GraphStateImplementation = GraphStateImplementation.empty
) extends GraphFunction

final case class Iterate(f: (Vertex) => Unit, iterations: Int, executeMessagedOnly: Boolean)
        extends GraphFunction

final case class IterateWithGraph(
    f: (Vertex, GraphState) => Unit,
    iterations: Int,
    executeMessagedOnly: Boolean,
    graphState: GraphStateImplementation = GraphStateImplementation.empty
)                                                     extends GraphFunction
final case class VertexFilter(f: (Vertex) => Boolean) extends GraphFunction

final case class VertexFilterWithGraph(
    f: (Vertex, GraphState) => Boolean,
    graphState: GraphStateImplementation = GraphStateImplementation.empty
)                                         extends GraphFunction
final case class Select(f: Vertex => Row) extends GraphFunction

final case class SelectWithGraph(
    f: (Vertex, GraphState) => Row,
    graphState: GraphStateImplementation = GraphStateImplementation.empty
) extends GraphFunction

final case class GlobalSelect(
    f: GraphState => Row,
    graphState: GraphStateImplementation = GraphStateImplementation.empty
)                                                      extends GraphFunction
final case class ExplodeSelect(f: Vertex => List[Row]) extends GraphFunction
final case class ClearChain()                          extends GraphFunction
final case class PerspectiveDone()                     extends GraphFunction

/**
  * {s}`GraphPerspective`
  *  : Public interface for graph algorithms
  *
  * The {s}`GraphPerspective` is the core interface of the algorithm API. It implements the operations exposed
  * by {s}`GraphOperations` returning a new {s}`GraphPerspective` for those operations that have a graph as a result.
  *
  * ```{seealso}
  * [](com.raphtory.core.algorithm.GraphOperations)
  * ```
  */
trait GraphPerspective extends GraphOperations[GraphPerspective]
