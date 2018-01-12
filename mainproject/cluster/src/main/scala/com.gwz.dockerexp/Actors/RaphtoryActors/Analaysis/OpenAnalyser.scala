package com.gwz.dockerexp.Actors.RaphtoryActors.Analaysis

import com.gwz.dockerexp.GraphEntities.{Edge, Vertex}

class OpenAnalyser(func: (Map[Int,Vertex],Map[(Int,Int),Edge])=> Object) extends java.io.Serializable {

  def analyse(vertices: Map[Int,Vertex],edges:Map[(Int,Int),Edge]):Object={
    func(vertices,edges)
  }

}
