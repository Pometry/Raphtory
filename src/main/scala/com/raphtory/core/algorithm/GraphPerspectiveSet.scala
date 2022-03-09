package com.raphtory.core.algorithm

import com.raphtory.core.graph.visitor.Vertex

trait GraphPerspectiveSet {
  def setGlobalState(f: (GraphState) => Unit): GraphPerspectiveSet
  def filter(f: (Vertex) => Boolean): GraphPerspectiveSet
  def filter(f: (Vertex, GraphState) => Boolean): GraphPerspectiveSet
  def step(f: (Vertex) => Unit): GraphPerspectiveSet
  def step(f: (Vertex, GraphState) => Unit): GraphPerspectiveSet

  def iterate(
      f: (Vertex) => Unit,
      iterations: Int,
      executeMessagedOnly: Boolean
  ): GraphPerspectiveSet

  def iterate(
      f: (Vertex, GraphState) => Unit,
      iterations: Int,
      executeMessagedOnly: Boolean
  ): GraphPerspectiveSet
  def select(f: Vertex => Row): Table
  def select(f: (Vertex, GraphState) => Row): Table
  def globalSelect(f: GraphState => Row): Table
  def explodeSelect(f: Vertex => List[Row]): Table
  def clearMessages(): GraphPerspectiveSet
  def transform(f: GraphPerspectiveSet => GraphPerspectiveSet): GraphPerspectiveSet = f(this)
  def transform(algorithm: GraphAlgorithm): GraphPerspectiveSet
  def execute(f: GraphPerspectiveSet => Table): Table                               = f(this)
  def execute(algorithm: GraphAlgorithm): Table
}
