package com.raphtory.allcommands

import org.scalatest.FunSuite
import akka.pattern.ask
import akka.util.Timeout
import com.raphtory.RaphtoryPD
import com.raphtory.algorithms.GraphState
import com.raphtory.core.components.querymanager.QueryManager.Message.{AreYouFinished, ManagingTask, PointQuery, RangeQuery, TaskFinished, Windows}
import com.raphtory.core.components.leader.WatermarkManager.Message.{WatermarkTime, WhatsTheTime}
import com.raphtory.core.model.algorithm.{GenericGraphPerspective, GraphAlgorithm}
import com.raphtory.spouts.FileSpout
import spray.json._
import com.google.common.hash.Hashing
import com.raphtory.algorithms.generic.ConnectedComponents

import java.io.File
import java.nio.charset.StandardCharsets
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._


//TODO Currently broken and needs to be updated with new comparison
class AllCommandsTest extends FunSuite {

  def downloadData(): Unit ={
    import sys.process._
    if(!new File("testupdates.txt").exists())
      "wget https://raw.githubusercontent.com/Raphtory/Data/main/testupdates.txt" !
  }

  downloadData()

  val testDir = "/tmp" //TODO CHANGE TO USER PARAM
  val node = RaphtoryPD(new FileSpout(".","testupdates.txt"),new AllCommandsBuilder())
  val watermarker     = node.getWatermarker()
  val watchdog        = node.getWatchdog()
  val queryManager    = node.getQueryManager()

  test("Warmup and Ingestion Test") {
        implicit val timeout: Timeout = 20.second
        try {
          var currentTimestamp = 0L
          var ingesting = true
          val startingTime = System.currentTimeMillis()
         while(ingesting){
            Thread.sleep(1000)
            val future = watermarker ? WhatsTheTime
            currentTimestamp = Await.result(future, timeout.duration).asInstanceOf[WatermarkTime].time
            if(currentTimestamp==299868) ingesting=false
          }
          println(s"Ingestion Took ${System.currentTimeMillis()-startingTime} milliseconds to complete")
          assert(currentTimestamp==299868) //all data is ingested and the minimum watermark is set to the last line in the data
        } catch {
      case _: java.util.concurrent.TimeoutException => assert(false)
    }
  }

  test("Graph State Test"){
    try {
      val result = algorithmTest(GraphState(testDir),1000)
      assert(result equals "1ecba4857ff7cf946a270d9e42f9035f774318437743078a24b8676fb68070c2")
    }
    catch {
      case _: java.util.concurrent.TimeoutException =>
        assert(false)
    }
  }

  test("Connected Components Test"){
    try {
      val result = algorithmTest(ConnectedComponents(testDir),1000)
      assert(result equals "fe1f5f87ad80941dd11448c1dfaf6aab2c45fab2f1ba9581d179124dc1ad2429")
    }
    catch {
      case _: java.util.concurrent.TimeoutException =>
        assert(false)
    }

  }


  def algorithmTest(algorithm: GraphAlgorithm,time:Int):String = {
    implicit val timeout: Timeout = time.second
    val queryName = getID(algorithm)
    val startingTime = System.currentTimeMillis()

    val future = queryManager ? RangeQuery(queryName,algorithm, 1, 290001, 10000, Windows(1000, 10000, 100000, 1000000))
    //val future = queryManager ? PointQuery(queryName,funcs, 299860)
    val taskManager = Await.result(future, timeout.duration).asInstanceOf[ManagingTask].actor
    val future2 = taskManager ? AreYouFinished

    val result = Await.result(future2, timeout.duration).asInstanceOf[TaskFinished].result
    println(s"Task completed in ${System.currentTimeMillis()-startingTime} milliseconds")
    val dir = new File(testDir+s"/$queryName").listFiles.filter(_.isFile)
    val results = (for(i <- dir) yield scala.io.Source.fromFile(i).getLines().toList).flatten.sorted(sortOrdering).flatten
    Hashing.sha256().hashString(results, StandardCharsets.UTF_8).toString
  }

  private def getID(algorithm:GraphAlgorithm):String = {
    try{
      val path= algorithm.getClass.getCanonicalName.split("\\.")
      path(path.size-1)+"_" + System.currentTimeMillis()
    }
    catch {
      case e:NullPointerException => "Anon_Func_"+System.currentTimeMillis()
    }
  }

  object sortOrdering extends Ordering[String] {
    def compare(line1: String, line2: String): Int = {
      val vertex1 = line1.split(",").take(3).map(_.toInt)
      val vertex2 = line2.split(",").take(3).map(_.toInt)
      if(vertex1(0) > vertex2(0)) 1
      else if (vertex1(0) < vertex2(0)) -1
      else{
        if(vertex1(1) > vertex2(1)) 1
        else if (vertex1(1) < vertex2(1)) -1
        else{
          if(vertex1(2) > vertex2(2)) 1
          else if (vertex1(2) < vertex2(2)) -1
          else 0
        }
      }

    }
  }
}



