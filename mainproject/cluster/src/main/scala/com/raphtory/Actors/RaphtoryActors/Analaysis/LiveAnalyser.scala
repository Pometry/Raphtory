package com.raphtory.Actors.RaphtoryActors.Analaysis

import com.raphtory.Actors.RaphtoryActors.RaphtoryActor
import com.raphtory.GraphEntities.{Edge, Vertex}

abstract class LiveAnalyser(name : String) extends RaphtoryActor {
  def analyse()
}
