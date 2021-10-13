package com.raphtory.allcommands

import akka.pattern.ask
import akka.util.Timeout
import com.raphtory.algorithms.newer.{ConnectedComponents, GraphState}
import com.raphtory.core.build.server.RaphtoryPD
import com.raphtory.core.components.leader.WatermarkManager.Message.{WatermarkTime, WhatsTheTime}
import com.raphtory.core.components.querymanager.QueryManager.Message.{AreYouFinished, ManagingTask, RangeQuery, TaskFinished}
import com.raphtory.resultcomparison.comparisonJsonProtocol._
import com.raphtory.spouts.FileSpout
import org.scalatest.FunSuite
import spray.json._
import scala.concurrent.Await
import scala.concurrent.duration._

class QueryVsAnalyser extends FunSuite {
  val node = RaphtoryPD(new FileSpout("src/test/scala/com/raphtory/data/allcommands","testupdates.txt"),new AllCommandsBuilder())
  val watermarker     = node.getWatermarker()
  val watchdog        = node.getWatchdog()
  val queryManager = node.getQueryManager()

  test("Warmup and Ingestion Test") {
        implicit val timeout: Timeout = 20.second
        try {
          var currentTimestamp = 0L
          Thread.sleep(60000) //Wait the initial watermarker warm up time
          for (i <- 1 to 6){
            Thread.sleep(10000)
            val future = watermarker ? WhatsTheTime
            currentTimestamp = Await.result(future, timeout.duration).asInstanceOf[WatermarkTime].time
          }
          assert(currentTimestamp==299868) //all data is ingested and the minimum watermark is set to the last line in the data
        } catch {
      case _: java.util.concurrent.TimeoutException => assert(false)
    }
  }

  test("Graph State Test"){

    try {
      //First we run the test and see if it finishes in a reasonable time
      implicit val timeout: Timeout = 180.second
      val future = queryManager ? RangeQuery(GraphState("/Users/bensteer/github/output"),1, 290001, 10000, List(1000, 10000, 100000, 1000000))
      val taskManager = Await.result(future, timeout.duration).asInstanceOf[ManagingTask].actor
      val future2 = taskManager ? AreYouFinished
      val result = Await.result(future2, timeout.duration).asInstanceOf[TaskFinished].result
      assert(true)
    }
    catch {
      case _: java.util.concurrent.TimeoutException => assert(false)
    }
  }

  test("Connected Components Test"){
    try {
      //First we run the test and see if it finishes in a reasonable time
      implicit val timeout: Timeout = 300.second
      val future = queryManager ? RangeQuery(ConnectedComponents("/Users/bensteer/github/output"), 1, 290001, 10000, List(1000, 10000, 100000, 1000000))
      val taskManager = Await.result(future, timeout.duration).asInstanceOf[ManagingTask].actor
      val future2 = taskManager ? AreYouFinished
      val result = Await.result(future2, timeout.duration).asInstanceOf[TaskFinished].result
      assert(true)
    }
    catch {
      case _: java.util.concurrent.TimeoutException => assert(false)
    }
  }





}
