package com.raphtory.dev.algorithms

import com.raphtory.RaphtoryComponent

import scala.language.postfixOps

object clusterTest extends App {
  val source = "com.raphtory.dev.wordSemantic.spouts.CoMatSpout"//spouts.FileSpout"
  val builder = "com.raphtory.dev.wordSemantic.graphbuilders.CoMatGB"//dev.blockchain.graphbuilders.chab_C2C_GB"
  new RaphtoryComponent("seedNode",1600)
  new RaphtoryComponent("watchdog",1601)
  new RaphtoryComponent("analysisManager",1602)
  new RaphtoryComponent("spout",1603,source)
  new RaphtoryComponent("router",1604,builder)
  new RaphtoryComponent("router",1605,builder)
//  new RaphtoryComponent("router",1606,builder)
//  new RaphtoryComponent("router",1607,builder)
//  new RaphtoryComponent("router",1608,builder)
//  new RaphtoryComponent("router",1609,builder)
//  new RaphtoryComponent("router",1610,builder)
//  new RaphtoryComponent("router",1611,builder)
//  new RaphtoryComponent("router",1612,builder)
//  new RaphtoryComponent("router",1613,builder)
  new RaphtoryComponent("partitionManager",1614)
  new RaphtoryComponent("partitionManager",1615)
//  new RaphtoryComponent("partitionManager",1616)
//  new RaphtoryComponent("partitionManager",1617)
//  new RaphtoryComponent("partitionManager",1618)
//  new RaphtoryComponent("partitionManager",1619)
//  new RaphtoryComponent("partitionManager",1620)
//  new RaphtoryComponent("partitionManager",1621)
//  new RaphtoryComponent("partitionManager",1622)
//  new RaphtoryComponent("partitionManager",1623)

}
