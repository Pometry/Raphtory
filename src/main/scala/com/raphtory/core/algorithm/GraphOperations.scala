package com.raphtory.core.algorithm

import com.raphtory.core.graph.visitor.Vertex

trait GraphOperations[G <: GraphOperations[G]] {
  def setGlobalState(f: (GraphState) => Unit): G
  def filter(f: (Vertex) => Boolean): G
  def filter(f: (Vertex, GraphState) => Boolean): G
  def step(f: (Vertex) => Unit): G
  def step(f: (Vertex, GraphState) => Unit): G
  def iterate(f: (Vertex) => Unit, iterations: Int, executeMessagedOnly: Boolean): G

  def iterate(
      f: (Vertex, GraphState) => Unit,
      iterations: Int,
      executeMessagedOnly: Boolean
  ): G
  def select(f: Vertex => Row): Table
  def select(f: (Vertex, GraphState) => Row): Table
  def globalSelect(f: GraphState => Row): Table
  def explodeSelect(f: Vertex => List[Row]): Table
  def clearMessages(): G
}
