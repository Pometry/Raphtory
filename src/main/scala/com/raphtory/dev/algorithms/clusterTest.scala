package com.raphtory.dev.algorithms

import com.raphtory.RaphtoryComponent

import scala.language.postfixOps

object clusterTest extends App {
  val partitionCount =2
  val routerCount =2
  val source = "com.raphtory.dev.wordSemantic.spouts.CoMatSpout"//spouts.FileSpout"
  val builder = "com.raphtory.dev.wordSemantic.graphbuilders.CoMatGB"//dev.blockchain.graphbuilders.chab_C2C_GB"
  new RaphtoryComponent("seedNode",partitionCount,routerCount,1600)
  new RaphtoryComponent("watchdog",partitionCount,routerCount,1601)
  new RaphtoryComponent("analysisManager",partitionCount,routerCount,1602)
  new RaphtoryComponent("spout",partitionCount,routerCount,1603,source)
  new RaphtoryComponent("router",partitionCount,routerCount,1604,builder)
  new RaphtoryComponent("router",partitionCount,routerCount,1605,builder)
//  new RaphtoryComponent("router",partitionCount,routerCount,1606,builder)
//  new RaphtoryComponent("router",partitionCount,routerCount,1607,builder)
//  new RaphtoryComponent("router",partitionCount,routerCount,1608,builder)
//  new RaphtoryComponent("router",partitionCount,routerCount,1609,builder)
//  new RaphtoryComponent("router",partitionCount,routerCount,1610,builder)
//  new RaphtoryComponent("router",partitionCount,routerCount,1611,builder)
//  new RaphtoryComponent("router",partitionCount,routerCount,1612,builder)
//  new RaphtoryComponent("router",partitionCount,routerCount,1613,builder)
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1614)
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1615)
//  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1616)
//  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1617)
//  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1618)
//  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1619)
//  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1620)
//  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1621)
//  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1622)
//  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1623)

}
