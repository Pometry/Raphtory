package com.raphtory.dev.allcommands

import com.raphtory.RaphtoryComponent
import scala.language.postfixOps

object AllCommandTest extends App {
  val partitionCount =10
  val routerCount =10
  new RaphtoryComponent("seedNode",partitionCount,routerCount,1600)
  new RaphtoryComponent("watchdog",partitionCount,routerCount,1601)
  new RaphtoryComponent("analysisManager",partitionCount,routerCount,1602)
  new RaphtoryComponent("spout",partitionCount,routerCount,1603,"com.raphtory.dev.allcommands.AllCommandsSpout")
  new RaphtoryComponent("router",partitionCount,routerCount,1604,"com.raphtory.dev.allcommands.AllCommandsBuilder")
  new RaphtoryComponent("router",partitionCount,routerCount,1605,"com.raphtory.dev.allcommands.AllCommandsBuilder")
  new RaphtoryComponent("router",partitionCount,routerCount,1606,"com.raphtory.dev.allcommands.AllCommandsBuilder")
  new RaphtoryComponent("router",partitionCount,routerCount,1607,"com.raphtory.dev.allcommands.AllCommandsBuilder")
  new RaphtoryComponent("router",partitionCount,routerCount,1608,"com.raphtory.dev.allcommands.AllCommandsBuilder")
  new RaphtoryComponent("router",partitionCount,routerCount,1609,"com.raphtory.dev.allcommands.AllCommandsBuilder")
  new RaphtoryComponent("router",partitionCount,routerCount,1610,"com.raphtory.dev.allcommands.AllCommandsBuilder")
  new RaphtoryComponent("router",partitionCount,routerCount,1611,"com.raphtory.dev.allcommands.AllCommandsBuilder")
  new RaphtoryComponent("router",partitionCount,routerCount,1612,"com.raphtory.dev.allcommands.AllCommandsBuilder")
  new RaphtoryComponent("router",partitionCount,routerCount,1613,"com.raphtory.dev.allcommands.AllCommandsBuilder")
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1614)
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1615)
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1616)
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1617)
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1618)
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1619)
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1620)
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1621)
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1622)
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1623)

}
