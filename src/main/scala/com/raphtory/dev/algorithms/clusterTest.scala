package com.raphtory.dev.algorithms

import com.raphtory.core.build.{RaphtoryComponentFactory, RaphtoryNode}
import com.raphtory.dev.wordSemantic.graphbuilders.CoMatGB
import com.raphtory.dev.wordSemantic.spouts.CoMatSpout

import scala.language.postfixOps

object clusterTest extends App {
  RaphtoryNode(new CoMatSpout(),new CoMatGB())
}
