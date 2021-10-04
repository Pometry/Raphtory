package com.raphtory.dev.gab
import com.raphtory.core.build.RaphtoryNode
import com.raphtory.dev.gab.graphbuilders.GabUserGraphBuilder
import com.raphtory.spouts.FileSpout

import scala.language.postfixOps

object GabDistributed extends App {
  RaphtoryNode(new FileSpout(),new GabUserGraphBuilder())
}

