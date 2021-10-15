package com.raphtory.dev.algorithms

import com.raphtory.core.build.server.RaphtoryPD
import com.raphtory.dev.wordSemantic.graphbuilders.CoMatGB
import com.raphtory.dev.wordSemantic.spouts.CoMatSpout

import scala.language.postfixOps

object clusterTest extends App {
  RaphtoryPD(new CoMatSpout(""),new CoMatGB())
}
