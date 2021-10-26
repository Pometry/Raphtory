package com.raphtory.allcommands

import org.scalatest.FunSuite
import akka.pattern.ask
import akka.util.Timeout
import com.raphtory.algorithms.ConnectedComponents
import com.raphtory.RaphtoryPD
import com.raphtory.algorithms.GraphState
import com.raphtory.core.components.querymanager.QueryManager.Message.{AreYouFinished, ManagingTask, PointQuery, RangeQuery, TaskFinished, Windows}
import com.raphtory.core.components.leader.WatermarkManager.Message.{WatermarkTime, WhatsTheTime}
import com.raphtory.core.implementations.objectgraph.algorithm.ObjectGraphPerspective
import com.raphtory.core.model.algorithm.GraphAlgorithm
import com.raphtory.resultcomparison.comparisonJsonProtocol._
import com.raphtory.resultcomparison.{ConnectedComponentsResults, RaphtoryResultComparitor, StateCheckResult, TimeParams, comparisonJsonProtocol}
import com.raphtory.spouts.FileSpout
import spray.json._
import com.google.common.hash.Hashing

import java.io.File
import java.nio.charset.StandardCharsets
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

//TODO Currently broken and needs to be updated with new comparison
class AllCommandsTest extends FunSuite {

  val testDir = "/Users/bensteer/github/output" //TODO CHANGE TO USER PARAM
  val node = RaphtoryPD(new FileSpout("src/test/scala/com/raphtory/data/allcommands","testupdates.txt"),new AllCommandsBuilder())
  val watermarker     = node.getWatermarker()
  val watchdog        = node.getWatchdog()
  val queryManager    = node.getQueryManager()

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

  test("Warmup and Ingestion Test") {
        implicit val timeout: Timeout = 20.second
        try {
          var currentTimestamp = 0L
          Thread.sleep(60000) //Wait the initial watermarker warm up time
          for (i <- 1 to 3){
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
      val result = algorithmTest(GraphState(testDir),500)
      println(result)
      //assert(result equals "e716531a7bd32c5713712533d648f4eeb8243bae8b9107780d47737f24e35477")
      //assert(result equals "f238fba1446eadb18f77b150bb0a8ec0b5da62faf29474fb37854264090f6f90")
      assert(result equals "1ecba4857ff7cf946a270d9e42f9035f774318437743078a24b8676fb68070c2")
    }
    catch {
      case _: java.util.concurrent.TimeoutException =>
        assert(false)
    }
  }

  test("Connected Components Test"){
    try {
      val result = algorithmTest(ConnectedComponents(testDir),300)
      println(result)
      //assert(result equals "3c2d7458ebce49f538fec7819ae8744f22f834d9c1a4be7e13d338ef2e7bfd58")
      //assert(result equals "1d374d99bb7fa7772de44af200731361779f969256a9febea55296393ecf2f8a")
      assert(result equals "238b0004e9c9468963beb44070cdd5ee8795e19596d9107397c4446391471430")
    }
    catch {
      case _: java.util.concurrent.TimeoutException =>
        assert(false)
    }

  }


  def algorithmTest(algorithm: GraphAlgorithm,time:Int):String = {
    implicit val timeout: Timeout = time.second
    val queryName = getID(algorithm)
    val funcs = getFuncs(algorithm)


    val future = queryManager ? RangeQuery(queryName,funcs, 1, 290001, 10000, Windows(1000, 10000, 100000, 1000000))
    //val future = queryManager ? PointQuery(queryName,funcs, 299860)
    val taskManager = Await.result(future, timeout.duration).asInstanceOf[ManagingTask].actor
    val future2 = taskManager ? AreYouFinished

    val result = Await.result(future2, timeout.duration).asInstanceOf[TaskFinished].result

    val dir = new File(testDir+s"/$queryName").listFiles.filter(_.isFile)
    val results = (for(i <- dir) yield scala.io.Source.fromFile(i).getLines().toList).flatten.sorted(sortOrdering).flatten
    Hashing.sha256().hashString(results, StandardCharsets.UTF_8).toString
  }

  private def getFuncs(graphAlgorithm: GraphAlgorithm) ={
    val graphPerspective = new ObjectGraphPerspective(0)
    graphAlgorithm.algorithm(graphPerspective)
    (graphPerspective.graphOpps.toList, graphPerspective.getTable().tableOpps.toList)
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


  test("hashtest") {

    val dir1 = new File(testDir+s"/sgs").listFiles.filter(_.isFile)
    val results1 = (for(i <- dir1) yield scala.io.Source.fromFile(i).getLines().toList).flatten.sorted(sortOrdering)

    val dir2 = new File(testDir+s"/scc").listFiles.filter(_.isFile)
    val results2 = (for(i <- dir2) yield scala.io.Source.fromFile(i).getLines().toList).flatten.sorted(sortOrdering)

    val dir3 = new File(testDir+s"/GraphState_1635258650746").listFiles.filter(_.isFile)
    val results3 = (for(i <- dir3) yield scala.io.Source.fromFile(i).getLines().toList).flatten.sorted(sortOrdering)

    val dir4 = new File(testDir+s"/ConnectedComponents_1635258687369").listFiles.filter(_.isFile)
    val results4 = (for(i <- dir4) yield scala.io.Source.fromFile(i).getLines().toList).flatten.sorted(sortOrdering)


    results2.foreach(i=>
     if(!results4.contains(i))
      println(i))

  }
}



