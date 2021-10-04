package com.raphtory.dev.lotr

import com.raphtory.dev.gab.graphbuilders.GabUserGraphBuilder

import scala.language.postfixOps
import scala.util.Random

object LOTRDistributed extends App {
  RaphtoryNode(new LOTRSpout(),new LOTRGraphBuilder())

}
