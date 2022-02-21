package com.raphtory.core.algorithm

import com.raphtory.core.components.querymanager.QueryManagement
import com.raphtory.core.graph.visitor.Vertex

sealed trait GraphFunction                 extends QueryManagement
final case class Step(f: (Vertex) => Unit) extends GraphFunction

final case class Iterate(f: (Vertex) => Unit, iterations: Int, executeMessagedOnly: Boolean)
        extends GraphFunction
final case class VertexFilter(f: (Vertex) => Boolean) extends GraphFunction
final case class Select(f: Vertex => Row)             extends GraphFunction
final case class ClearChain()                         extends GraphFunction

abstract class GraphPerspective{
  def setup(f: GraphAccumulator => GraphAccumulator): GraphPerspective
  def filter(f: (Vertex) => Boolean): GraphPerspective
  def filter(f: (Vertex, GraphAccumulator) => Boolean): GraphPerspective
  def step(f: (Vertex) => Unit): GraphPerspective
  def step(f: (Vertex, GraphAccumulator) => Unit): GraphPerspective
  def iterate(f: (Vertex) => Unit, iterations: Int, executeMessagedOnly: Boolean): GraphPerspective
  def iterate(f: (Vertex, GraphAccumulator) => Unit, iterations: Int, executeMessagedOnly: Boolean): GraphPerspective
  def select(f: Vertex => Row): Table
  def select(f: (Vertex, GraphAccumulator) => Row): Table
  //MetaData
  def nodeCount(): Int
}
