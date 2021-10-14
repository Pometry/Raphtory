package com.raphtory.core.model.algorithm

import com.raphtory.core.model.graph.visitor.Vertex

sealed trait GraphFunction
case   class Step(f:(Vertex)=>Unit) extends GraphFunction
case   class Iterate(f:(Vertex)=>Unit, iterations:Int) extends GraphFunction
case   class VertexFilter(f:(Vertex)=>Boolean) extends GraphFunction
case   class Select(f:Vertex=>Row) extends GraphFunction

abstract class GraphPerspective{
  def filter(f:(Vertex)=>Boolean):GraphPerspective
  def step(f:(Vertex)=>Unit):GraphPerspective
  def iterate(f:(Vertex)=>Unit, iterations:Int):GraphPerspective
  def select(f:Vertex=>Row):Table


  //MetaData
  def nodeCount():Int
}