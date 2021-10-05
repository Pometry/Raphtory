package com.raphtory.dev.lotr

import com.raphtory.core.build.server.RaphtoryPD
import com.raphtory.dev.gab.graphbuilders.GabUserGraphBuilder

import scala.language.postfixOps
import scala.util.Random

object LOTRDistributed extends App {
  RaphtoryPD(new LOTRSpout(),new LOTRGraphBuilder())

}
