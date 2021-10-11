package com.raphtory.allcommands

import com.raphtory.algorithms.old.sortOrdering
import com.raphtory.resultcomparison.StateCheckResult
import spray.json._
import java.io.File


import akka.actor.ActorRef
import org.scalatest.FunSuite
import akka.pattern.ask
import akka.util.Timeout
import com.raphtory.algorithms.old.{ConnectedComponents, StateTest}
import com.raphtory.core.build.server.RaphtoryPD
import com.raphtory.core.components.querymanager.QueryManager.Message.{AreYouFinished, ManagingTask, TaskFinished}
import com.raphtory.core.components.analysismanager.AnalysisRestApi.message.RangeAnalysisRequest
import com.raphtory.core.components.leader.WatermarkManager.Message.{WatermarkTime, WhatsTheTime}
import com.raphtory.core.model.algorithm.Analyser
import com.raphtory.resultcomparison.comparisonJsonProtocol._
import com.raphtory.resultcomparison.{ConnectedComponentsResults, RaphtoryResultComparitor, StateCheckResult, TimeParams, comparisonJsonProtocol}
import com.raphtory.serialisers.DefaultSerialiser
import com.raphtory.spouts.FileSpout
import spray.json._

import java.io.File
import scala.collection.mutable.HashMap
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._


object TempComparitor extends App {
  val testData = groupConnectedComponentsData(new File("/Users/bensteer/github/output/ConnectedComponents_1633990578443"))

  val standardData = HashMap[TimeParams, ConnectedComponentsResults]() ++=
    scala.io.Source.fromFile("src/test/scala/com/raphtory/data/allcommands/connectedcomponents.json").getLines()
      .map(line => line.parseJson.convertTo[ConnectedComponentsResults])
      .map(state => (TimeParams(state.time, state.windowsize), state))

  val correctResults = testData.map(row => row._2.compareTo(standardData(row._1))).fold(true) { (x, y) => x && y }
  println(correctResults)

  def groupConnectedComponentsData(folder: File) = {
   val x= (for (fileEntry <- folder.listFiles())
      yield scala.io.Source.fromFile(fileEntry).getLines().toArray)
      .flatten
      .groupBy(vertex => {
        val line = vertex.split(",")
        (line(0),line(1))
      })
      .map{
        case ((timestamp,window),lines) =>
          val groups = lines.groupBy(line => line.split(",")(3)).map(x=> (x._1,x._2.size))
          val groupedNonIslands = groups.filter(x => x._2 > 1)
          val biggest = groups.maxBy(_._2)._2
          val sorted = groupedNonIslands.toArray.sortBy(_._2)(sortOrdering).map(x=>x._2)
          val top5 = if(sorted.length<=5) sorted else sorted.take(5)
          val total = groups.size
          val totalWithoutIslands = groupedNonIslands.size
          val totalIslands = total - totalWithoutIslands
          val proportion = biggest.toFloat / groups.values.sum
          val totalGT2 = groups.count(x => x._2 > 2)
          val quote = """""""
          val result = s"{${quote}proportion${quote}:${proportion},${quote}clustersGT2${quote}:${totalGT2},${quote}windowsize${quote}:${window},${quote}totalIslands${quote}:${totalIslands},${quote}total${quote}:${total},${quote}top5${quote}:${top5.mkString("[",",","]")},${quote}viewTime${quote}:32,${quote}time${quote}:${timestamp}}"
          (TimeParams(timestamp.toLong,window.toLong),result.parseJson.convertTo[ConnectedComponentsResults])
      }
    x
  }

}
