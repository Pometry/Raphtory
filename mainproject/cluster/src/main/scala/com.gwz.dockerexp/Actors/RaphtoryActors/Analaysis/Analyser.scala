package com.gwz.dockerexp.Actors.RaphtoryActors.Analaysis

import com.gwz.dockerexp.GraphEntities.{Edge, Vertex}

trait Analyser extends java.io.Serializable{
  def analyse(vertices:Map[Int,Vertex],edges:Map[(Int,Int),Edge]):Object
}
