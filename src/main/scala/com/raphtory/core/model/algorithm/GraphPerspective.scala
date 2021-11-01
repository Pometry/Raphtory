package com.raphtory.core.model.algorithm

import com.raphtory.core.model.graph.visitor.Vertex

sealed trait GraphFunction
final case class Step(f:(Vertex)=>Unit) extends GraphFunction
final case class Iterate(f:(Vertex)=>Unit, iterations:Int,executeMessagedOnly:Boolean) extends GraphFunction
final case class VertexFilter(f:(Vertex)=>Boolean) extends GraphFunction
final case class Select(f:Vertex=>Row) extends GraphFunction

abstract class GraphPerspective{
  def filter(f:(Vertex)=>Boolean):GraphPerspective
  def step(f:(Vertex)=>Unit):GraphPerspective
  def iterate(f:(Vertex)=>Unit, iterations:Int,executeMessagedOnly:Boolean):GraphPerspective
  def select(f:Vertex=>Row):Table


  //MetaData
  def nodeCount():Int
}