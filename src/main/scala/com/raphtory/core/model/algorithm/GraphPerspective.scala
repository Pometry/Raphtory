package com.raphtory.core.model.algorithm

import com.raphtory.core.model.graph.visitor.Vertex

sealed trait RaphtoryFunction
case   class Step(f:(Vertex)=>Unit) extends RaphtoryFunction
case   class Iterate(f:(Vertex)=>Unit, iterations:Int) extends RaphtoryFunction
case   class Filter(f:(Vertex)=>Boolean) extends RaphtoryFunction

abstract class GraphPerspective{
  def filter(f:(Vertex)=>Boolean):GraphPerspective
  def step(f:(Vertex)=>Unit):GraphPerspective
  def iterate(f:(Vertex)=>Unit, iterations:Int):GraphPerspective
  def select[S](f:Vertex=>S):Table
}