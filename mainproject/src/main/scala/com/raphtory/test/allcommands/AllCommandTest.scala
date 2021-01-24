package com.raphtory.test.allcommands

import com.raphtory.RaphtoryComponent
import scala.language.postfixOps

object AllCommandTest extends App {
  val partitionCount =2
  val routerCount =2
  new RaphtoryComponent("seedNode",partitionCount,routerCount,1600)
  new RaphtoryComponent("watchdog",partitionCount,routerCount,1601)
  new RaphtoryComponent("analysisManager",partitionCount,routerCount,1602)
  new RaphtoryComponent("spout",partitionCount,routerCount,1603,"com.raphtory.test.allcommands.AllCommandsSpout")
  new RaphtoryComponent("router",partitionCount,routerCount,1604,"com.raphtory.test.allcommands.AllCommandsBuilder")
  new RaphtoryComponent("router",partitionCount,routerCount,1605,"com.raphtory.test.allcommands.AllCommandsBuilder")
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1606)
  new RaphtoryComponent("partitionManager",partitionCount,routerCount,1607)


}

//Wanna check are all messages being ingested
//Is there a slow down over time (full range vs the final point in  time)
//Do changes make a difference to the run times
//How can we track the time taken by each stage of the analysis superstep


