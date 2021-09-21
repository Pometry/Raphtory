package com.raphtory.allcommands

import akka.actor.ActorRef
import com.raphtory.RaphtoryComponent
import org.scalatest.FunSuite
import akka.pattern.ask
import akka.util.Timeout
import com.raphtory.algorithms.{ConnectedComponents, StateTest}
import com.raphtory.core.actors.analysismanager.AnalysisManager.Message.{AreYouFinished, ManagingTask, TaskFinished}
import com.raphtory.core.actors.analysismanager.AnalysisRestApi.message.RangeAnalysisRequest
import com.raphtory.core.actors.orchestration.raphtoryleader.WatermarkManager.Message.{WatermarkTime, WhatsTheTime}
import com.raphtory.core.analysis.api.Analyser
import com.raphtory.resultcomparison.comparisonJsonProtocol._
import com.raphtory.resultcomparison.{ConnectedComponentsResults, RaphtoryResultComparitor, StateCheckResult, TimeParams, comparisonJsonProtocol}
import com.raphtory.serialisers.DefaultSerialiser
import spray.json._

import java.io.File
import scala.collection.mutable.HashMap
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

class AllCommandsTest extends FunSuite {
  //set FILE_SPOUT_DIRECTORY=src/test/scala/com/raphtory/data/allcommands
  //    FILE_SPOUT_FILENAME=testupdates.txt
  //    OUTPUT_PATH=src/test/scala/com/raphtory/data/allcommands/output

  val leader = new RaphtoryComponent("leader",1600)
  val analysisManager = new RaphtoryComponent("analysisManager",1602)
  val spout= new RaphtoryComponent("spout",1603,"com.raphtory.spouts.FileSpout")
  val builder1 = new RaphtoryComponent("builder",1604,"com.raphtory.allcommands.AllCommandsBuilder")
  val builder2 = new RaphtoryComponent("builder",1605,"com.raphtory.allcommands.AllCommandsBuilder")
  val builder3 = new RaphtoryComponent("builder",1606,"com.raphtory.allcommands.AllCommandsBuilder")
  val pm1 = new RaphtoryComponent("partitionManager",1614)
  val pm2 = new RaphtoryComponent("partitionManager",1615)
  val pm3 = new RaphtoryComponent("partitionManager",1616)
  val pm4 = new RaphtoryComponent("partitionManager",1617)



  test("Warmup and Ingestion Test") {
        implicit val timeout: Timeout = 20.second
        try {
          var currentTimestamp = 0L
          Thread.sleep(60000) //Wait the initial watermarker warm up time
          for (i <- 1 to 6){
            Thread.sleep(10000)
            val future = leader.getWatermarker.get ? WhatsTheTime
            currentTimestamp = Await.result(future, timeout.duration).asInstanceOf[WatermarkTime].time
          }
          assert(currentTimestamp==299868) //all data is ingested and the minimum watermark is set to the last line in the data
        } catch {
      case _: java.util.concurrent.TimeoutException => assert(false)
    }
  }

  test("Graph State Test"){
    val stateTest = new StateTest(Array()).getClass.getCanonicalName
    val serialiser = new DefaultSerialiser().getClass.getCanonicalName
    try {
      //First we run the test and see if it finishes in a reasonable time
      implicit val timeout: Timeout = 180.second
      val future = analysisManager.getAnalysisManager.get ? RangeAnalysisRequest(stateTest, serialiser, 1, 290001, 10000, List(1000, 10000, 100000, 1000000), Array())
      val taskManager = Await.result(future, timeout.duration).asInstanceOf[ManagingTask].actor
      val future2 = taskManager ? AreYouFinished
      val result = Await.result(future2, timeout.duration).asInstanceOf[TaskFinished].result

      val testData = HashMap[TimeParams, StateCheckResult]() ++=
        scala.io.Source.fromFile("src/test/scala/com/raphtory/data/allcommands/output").getLines()
          .map(line => line.parseJson.convertTo[StateCheckResult])
          .map(state => (TimeParams(state.time, state.windowsize), state))
      val standardData = HashMap[TimeParams, StateCheckResult]() ++=
        scala.io.Source.fromFile("src/test/scala/com/raphtory/data/allcommands/statetest.json").getLines()
          .map(line => line.parseJson.convertTo[StateCheckResult])
          .map(state => (TimeParams(state.time, state.windowsize), state))

      val correctResults = testData.map(row => row._2.compareTo(standardData(row._1))).fold(true) { (x, y) => x && y }
      if (correctResults) {
        new File("src/test/scala/com/raphtory/data/allcommands/output").delete()
      }
      assert(correctResults)
    }
    catch {
      case _: java.util.concurrent.TimeoutException => assert(false)
    }
  }

  test("Connected Components Test"){
    val connectedComponents = ConnectedComponents().getClass.getCanonicalName
    val serialiser = new DefaultSerialiser().getClass.getCanonicalName
    try {
      //First we run the test and see if it finishes in a reasonable time
      implicit val timeout: Timeout = 300.second
      val future = analysisManager.getAnalysisManager.get ? RangeAnalysisRequest(connectedComponents, serialiser, 1, 290001, 10000, List(1000, 10000, 100000, 1000000), Array())
      val taskManager = Await.result(future, timeout.duration).asInstanceOf[ManagingTask].actor
      val future2 = taskManager ? AreYouFinished
      val result = Await.result(future2, timeout.duration).asInstanceOf[TaskFinished].result

      val testData = HashMap[TimeParams, ConnectedComponentsResults]() ++=
        scala.io.Source.fromFile("src/test/scala/com/raphtory/data/allcommands/output").getLines()
          .map(line => line.parseJson.convertTo[ConnectedComponentsResults])
          .map(state => (TimeParams(state.time, state.windowsize), state))
      val standardData = HashMap[TimeParams, ConnectedComponentsResults]() ++=
        scala.io.Source.fromFile("src/test/scala/com/raphtory/data/allcommands/connectedcomponents.json").getLines()
          .map(line => line.parseJson.convertTo[ConnectedComponentsResults])
          .map(state => (TimeParams(state.time, state.windowsize), state))

      val correctResults = testData.map(row => row._2.compareTo(standardData(row._1))).fold(true) { (x, y) => x && y }
      if (correctResults) {
        new File("src/test/scala/com/raphtory/data/allcommands/output").delete()
      }
      assert(correctResults)
    }
    catch {
      case _: java.util.concurrent.TimeoutException => assert(false)
    }
  }





}
