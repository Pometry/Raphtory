package com.raphtory.core.implementations.objectgraph.algorithm

import com.raphtory.core.model.algorithm.{GraphFunction, GraphPerspective, Iterate, Row, Select, Step, Table, VertexFilter}
import com.raphtory.core.model.graph.visitor.Vertex

import scala.collection.mutable

class ClosureEncap {
  def closureFunction(x:(Vertex => Boolean))(gen: (Vertex => Boolean) => (Vertex => Boolean)) = gen(x)
  def filterClosure(x:(Vertex => Boolean)) = closureFunction(x) { enclosed =>
    x   // Desired function, with 'v1' and 'v2' enclosed
  }
}

class ObjectGraphPerspective(vertices:Int)  extends GraphPerspective{
  val graphOpps = mutable.Queue[GraphFunction]()
  val table     = new ObjectTable()

  def bulkAdd(graphFuncs:List[GraphFunction]) = graphFuncs.foreach(f=> graphOpps.enqueue(f))

  override def filter(f: Vertex => Boolean): GraphPerspective = {
    val func = new ClosureEncap().filterClosure(f)
    graphOpps.enqueue(VertexFilter(func))
    this
  }

  override def step(f: Vertex => Unit): GraphPerspective = {
    def closurefunc(v:Vertex):Unit = f(v)
    graphOpps.enqueue(Step(closurefunc))
    this
  }

  override def iterate(f: Vertex => Unit, iterations: Int): GraphPerspective = {
    def closurefunc(v:Vertex):Unit = f(v)
    graphOpps.enqueue(Iterate(closurefunc,iterations))
    this
  }

  override def select(f: Vertex => Row): Table = {
    def closurefunc(v:Vertex):Row = f(v)
    graphOpps.enqueue(Select(closurefunc))
    table
  }

  def getNextOperation():Option[GraphFunction] = if (graphOpps.nonEmpty) Some(graphOpps.dequeue()) else None

  def getTable() = table

  override def nodeCount(): Int = vertices

}
