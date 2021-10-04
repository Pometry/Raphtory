package com.raphtory.dev.gab
import com.raphtory.core.build.server.RaphtoryPD
import com.raphtory.dev.gab.graphbuilders.GabUserGraphBuilder
import com.raphtory.spouts.FileSpout

import scala.language.postfixOps

object GabDistributed extends App {
  RaphtoryPD(new FileSpout(),new GabUserGraphBuilder())
}

