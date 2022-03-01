package com.raphtory.core.algorithm

import com.raphtory.core.graph.visitor.Vertex

import scala.collection.mutable

class GenericGraphPerspective(vertices: Int) extends GraphPerspective {
  val graphOpps = mutable.Queue[GraphFunction]()
  val table     = new GenericTable()

  override def setGlobalState(f: GraphState => Unit): GraphPerspective = {
    graphOpps.enqueue(Setup(f))
    this
  }

  override def filter(f: Vertex => Boolean): GraphPerspective = {
    graphOpps.enqueue(VertexFilter(f))
    this
  }

  override def filter(f: (Vertex, GraphState) => Boolean): GraphPerspective = {
    graphOpps.enqueue(VertexFilterWithGraph(f))
    this
  }

  override def step(f: Vertex => Unit): GraphPerspective = {
    graphOpps.enqueue(Step(f))
    this
  }

  override def step(f: (Vertex, GraphState) => Unit): GraphPerspective = {
    graphOpps.enqueue(StepWithGraph(f))
    this
  }

  override def iterate(
      f: Vertex => Unit,
      iterations: Int,
      executeMessagedOnly: Boolean
  ): GraphPerspective = {
    graphOpps.enqueue(Iterate(f, iterations, executeMessagedOnly))
    this
  }

  override def iterate(
      f: (Vertex, GraphState) => Unit,
      iterations: Int,
      executeMessagedOnly: Boolean
  ): GraphPerspective = {
    graphOpps.enqueue(
            IterateWithGraph(f, iterations, executeMessagedOnly)
    )
    this
  }

  override def select(f: Vertex => Row): Table = {
    graphOpps.enqueue(Select(f))
    table
  }

  override def select(f: (Vertex, GraphState) => Row): Table = {
    graphOpps.enqueue(SelectWithGraph(f))
    table
  }

  override def globalSelect(f: GraphState => Row): Table = {
    graphOpps.enqueue(GlobalSelect(f))
    table
  }

  override def explodeSelect(f: Vertex => List[Row]): Table = {
    graphOpps.enqueue(ExplodeSelect(f))
    table
  }

  def clearMessages(): GraphPerspective = {
    graphOpps.enqueue(ClearChain())
    this
  }

  def getNextOperation(): GraphFunction =
    if (graphOpps.nonEmpty) graphOpps.dequeue() else PerspectiveDone()

  def getTable() = table

  override def nodeCount(): Int = vertices

}
