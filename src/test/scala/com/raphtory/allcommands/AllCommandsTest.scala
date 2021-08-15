package com.raphtory.allcommands

import com.raphtory.RaphtoryComponent
import org.scalatest.FunSuite
import akka.pattern.ask
import akka.util.Timeout
import com.raphtory.core.actors.orchestration.clustermanager.WatermarkManager.Message.{WatermarkTime, WhatsTheTime}

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

class AllCommandsTest extends FunSuite {
  //SET
  val partitionCount =4
  val routerCount =3
  val seedNode = new RaphtoryComponent("seedNode",partitionCount,routerCount,1600)
  val analysisManager = new RaphtoryComponent("analysisManager",partitionCount,routerCount,1602)
  val spout= new RaphtoryComponent("spout",partitionCount,routerCount,1603,"com.raphtory.spouts.FileSpout")
  val router1 = new RaphtoryComponent("router",partitionCount,routerCount,1604,"com.raphtory.allcommands.AllCommandsBuilder")
  val router2 = new RaphtoryComponent("router",partitionCount,routerCount,1605,"com.raphtory.allcommands.AllCommandsBuilder")
  val router3 = new RaphtoryComponent("router",partitionCount,routerCount,1606,"com.raphtory.allcommands.AllCommandsBuilder")
  val pm1 = new RaphtoryComponent("partitionManager",partitionCount,routerCount,1614)
  val pm2 = new RaphtoryComponent("partitionManager",partitionCount,routerCount,1615)
  val pm3 = new RaphtoryComponent("partitionManager",partitionCount,routerCount,1616)
  val pm4 = new RaphtoryComponent("partitionManager",partitionCount,routerCount,1617)

  test("Watermark Test") {
      implicit val timeout: Timeout = 20.second
      try {
        val future = seedNode.getWatermarker.get ? WhatsTheTime
        val timestamp = Await.result(future, timeout.duration).asInstanceOf[WatermarkTime].time
        println(timestamp)
        assert(timestamp==timestamp)
    } catch {
      case _: java.util.concurrent.TimeoutException => assert(false)
    }
  }
}
