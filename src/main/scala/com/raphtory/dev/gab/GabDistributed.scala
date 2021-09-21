package com.raphtory.dev.gab
import com.raphtory.core.build.RaphtoryComponent

import scala.language.postfixOps

object GabDistributed extends App {
  new RaphtoryComponent("leader",1600)
  new RaphtoryComponent("analysisManager",1602)
  new RaphtoryComponent("spout",1603,"com.raphtory.dev.lotr.LOTRSpout")
  new RaphtoryComponent("builder",1604,"com.raphtory.dev.gab.graphbuilders.GabUserGraphBuilder")
  new RaphtoryComponent("builder",1605,"com.raphtory.dev.gab.graphbuilders.GabUserGraphBuilder")
  new RaphtoryComponent("builder",1606,"com.raphtory.dev.gab.graphbuilders.GabUserGraphBuilder")
  new RaphtoryComponent("partitionManager",1614)
  new RaphtoryComponent("partitionManager",1615)
  new RaphtoryComponent("partitionManager",1616)
  new RaphtoryComponent("partitionManager",1617)

}

