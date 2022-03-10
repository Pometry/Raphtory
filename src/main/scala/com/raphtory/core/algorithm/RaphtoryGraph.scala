package com.raphtory.core.algorithm

import com.raphtory.core.graph.visitor.Vertex

trait RaphtoryGraph {
  def setGlobalState(f: (GraphState) => Unit): RaphtoryGraph
  def filter(f: (Vertex) => Boolean): RaphtoryGraph
  def filter(f: (Vertex, GraphState) => Boolean): RaphtoryGraph
  def step(f: (Vertex) => Unit): RaphtoryGraph
  def step(f: (Vertex, GraphState) => Unit): RaphtoryGraph

  def iterate(
      f: (Vertex) => Unit,
      iterations: Int,
      executeMessagedOnly: Boolean
  ): RaphtoryGraph

  def iterate(
      f: (Vertex, GraphState) => Unit,
      iterations: Int,
      executeMessagedOnly: Boolean
  ): RaphtoryGraph
  def select(f: Vertex => Row): Table
  def select(f: (Vertex, GraphState) => Row): Table
  def globalSelect(f: GraphState => Row): Table
  def explodeSelect(f: Vertex => List[Row]): Table
  def clearMessages(): RaphtoryGraph
  def transform(f: RaphtoryGraph => RaphtoryGraph): RaphtoryGraph = f(this)
  def transform(algorithm: GraphAlgorithm): RaphtoryGraph
  def execute(f: RaphtoryGraph => Table): Table                   = f(this)
  def execute(algorithm: GraphAlgorithm): Table
}
