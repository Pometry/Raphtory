package com.raphtory.core.algorithm

import com.raphtory.core.components.querymanager.QueryManagement
import com.raphtory.core.graph.visitor.Vertex

sealed trait GraphFunction                 extends QueryManagement
final case class Step(f: (Vertex) => Unit) extends GraphFunction

final case class Iterate(f: (Vertex) => Unit, iterations: Int, executeMessagedOnly: Boolean)
        extends GraphFunction
final case class VertexFilter(f: (Vertex) => Boolean) extends GraphFunction
final case class Select(f: Vertex => Row)             extends GraphFunction
final case class ExplodeSelect(f: Vertex => List[Row]) extends GraphFunction
final case class ClearChain()                         extends GraphFunction

abstract class GraphPerspective {
  def filter(f: (Vertex) => Boolean): GraphPerspective
  def step(f: (Vertex) => Unit): GraphPerspective
  def iterate(f: (Vertex) => Unit, iterations: Int, executeMessagedOnly: Boolean): GraphPerspective
  def select(f: Vertex => Row): Table
  def explodeSelect(f: Vertex => List[Row]): Table
  //MetaData
  def nodeCount(): Int
}
