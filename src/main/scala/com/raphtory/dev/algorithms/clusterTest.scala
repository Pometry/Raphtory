package com.raphtory.dev.algorithms

import com.raphtory.core.build.server.RaphtoryGraph
import com.raphtory.dev.wordSemantic.graphbuilders.CoMatGB
import com.raphtory.dev.wordSemantic.spouts.CoMatSpout

import scala.language.postfixOps

object clusterTest extends App {
  RaphtoryGraph(new CoMatSpout(""),new CoMatGB())
}
