package com.raphtory.core.algorithm

import com.raphtory.core.components.querymanager.QueryManagement
import com.raphtory.core.graph.visitor.Vertex

sealed trait GraphFunction extends QueryManagement
final case class Setup(f: GraphState => Unit) extends GraphFunction
final case class Step(f: (Vertex) => Unit) extends GraphFunction
final case class StepWithGraph(f: (Vertex, GraphState) => Unit,
                               graphState: Option[GraphState])
    extends GraphFunction
final case class Iterate(f: (Vertex) => Unit,
                         iterations: Int,
                         executeMessagedOnly: Boolean)
    extends GraphFunction
final case class IterateWithGraph(f: (Vertex, GraphState) => Unit,
                                  iterations: Int,
                                  executeMessagedOnly: Boolean,
                                  graphState: Option[GraphState])
    extends GraphFunction
final case class VertexFilter(f: (Vertex) => Boolean) extends GraphFunction
final case class VertexFilterWithGraph(f: (Vertex, GraphState) => Boolean,
                                       graphState: Option[GraphState])
    extends GraphFunction
final case class Select(f: Vertex => Row) extends GraphFunction
final case class SelectWithGraph(f: (Vertex, GraphState) => Row,
                                 graphState: Option[GraphState])
    extends GraphFunction
final case class ClearChain() extends GraphFunction

abstract class GraphPerspective {
  def setGlobalState(f: (GraphState) => Unit): GraphPerspective
  def filter(f: (Vertex) => Boolean): GraphPerspective
  def filter(f: (Vertex, GraphState) => Boolean): GraphPerspective
  def step(f: (Vertex) => Unit): GraphPerspective
  def step(f: (Vertex, GraphState) => Unit): GraphPerspective
  def iterate(f: (Vertex) => Unit,
              iterations: Int,
              executeMessagedOnly: Boolean): GraphPerspective
  def iterate(f: (Vertex, GraphState) => Unit,
              iterations: Int,
              executeMessagedOnly: Boolean): GraphPerspective
  def select(f: Vertex => Row): Table
  def select(f: (Vertex, GraphState) => Row): Table

  //MetaData
  def nodeCount(): Int
}
