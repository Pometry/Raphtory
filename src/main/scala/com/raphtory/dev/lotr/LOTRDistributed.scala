package com.raphtory.dev.lotr

import com.raphtory.RaphtoryComponent
import scala.language.postfixOps

object LOTRDistributed extends App {
  val partitionCount =3
  val routerCount =3
  new RaphtoryComponent("seedNode",partitionCount,routerCount,1600)
  new RaphtoryComponent("watchdog",partitionCount,routerCount,1601)
  new RaphtoryComponent("analysisManager",partitionCount,routerCount,1602)
  new RaphtoryComponent("spout",partitionCount,routerCount,1603,"com.raphtory.dev.lotr.LOTRSpout")
  new RaphtoryComponent("router",partitionCount,routerCount,1604,"com.raphtory.dev.lotr.LOTRGraphBuilder")
  new RaphtoryComponent("router",partitionCount,routerCount,1605,"com.raphtory.dev.lotr.LOTRGraphBuilder")
  new RaphtoryComponent("router",partitionCount,routerCount,1606,"com.raphtory.dev.lotr.LOTRGraphBuilder")
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1614)
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1615)
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1616)

}
