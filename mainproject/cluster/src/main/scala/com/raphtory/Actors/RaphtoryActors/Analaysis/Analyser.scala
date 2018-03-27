package com.raphtory.Actors.RaphtoryActors.Analaysis

import com.raphtory.GraphEntities.{Edge, Vertex}

trait Analyser extends java.io.Serializable{
  def analyse(vertices:Map[Int,Vertex],edges:Map[(Int,Int),Edge]):Object
}
