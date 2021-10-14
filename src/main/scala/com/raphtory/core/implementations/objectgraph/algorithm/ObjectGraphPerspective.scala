package com.raphtory.core.implementations.objectgraph.algorithm

import com.raphtory.core.model.algorithm.{GraphFunction, GraphPerspective, Iterate, Row, Step, Table, VertexFilter,Select}
import com.raphtory.core.model.graph.visitor.Vertex

import scala.collection.mutable


class ObjectGraphPerspective(vertices:Int)  extends GraphPerspective{
  val graphOpps = mutable.Queue[GraphFunction]()
  val table     = new ObjectTable()

  override def filter(f: Vertex => Boolean): GraphPerspective = {
    graphOpps.enqueue(VertexFilter(f))
    this
  }

  override def step(f: Vertex => Unit): GraphPerspective = {
    graphOpps.enqueue(Step(f))
    this
  }

  override def iterate(f: Vertex => Unit, iterations: Int): GraphPerspective = {
    graphOpps.enqueue(Iterate(f,iterations))
    this
  }

  override def select(f: Vertex => Row): Table = {
    graphOpps.enqueue(Select(f))
    table
  }

  def getNextOperation():Option[GraphFunction] = if (graphOpps.nonEmpty) Some(graphOpps.dequeue()) else None

  def getTable() = table

  override def nodeCount(): Int = vertices

}
