package com.raphtory.dev.lotr

import com.raphtory.RaphtoryComponent

import scala.language.postfixOps
import scala.util.Random

object LOTRDistributed extends App {
  new RaphtoryComponent("seedNode",1600)
  new RaphtoryComponent("analysisManager",1602)
  new RaphtoryComponent("spout",1603,"com.raphtory.dev.lotr.LOTRSpout")
  new RaphtoryComponent("router",1604,"com.raphtory.dev.lotr.LOTRGraphBuilder")
  new RaphtoryComponent("router",1605,"com.raphtory.dev.lotr.LOTRGraphBuilder")
  new RaphtoryComponent("router",1606,"com.raphtory.dev.lotr.LOTRGraphBuilder")
  new RaphtoryComponent("partitionManager",1614)
  new RaphtoryComponent("partitionManager",1615)
  new RaphtoryComponent("partitionManager",1616)
  new RaphtoryComponent("partitionManager",1617)
  //new RaphtoryComponent("partitionManager",partitionCount,routerCount,1615)
  //new RaphtoryComponent("partitionManager",partitionCount,routerCount,1616)

}
