package com.raphtory.allcommands

import com.raphtory.algorithms.old.{Analyser, ConnectedComponents, StateTest, sortOrdering}
import com.raphtory.resultcomparison.StateCheckResult
import spray.json._

import java.io.File
import akka.actor.ActorRef
import org.scalatest.FunSuite
import akka.pattern.ask
import akka.util.Timeout
import com.raphtory.core.build.server.RaphtoryPD
import com.raphtory.core.components.querymanager.QueryManager.Message.{AreYouFinished, ManagingTask, TaskFinished}
import com.raphtory.core.components.leader.WatermarkManager.Message.{WatermarkTime, WhatsTheTime}
import com.raphtory.resultcomparison.comparisonJsonProtocol._
import com.raphtory.resultcomparison.{ConnectedComponentsResults, RaphtoryResultComparitor, StateCheckResult, TimeParams, comparisonJsonProtocol}
import com.raphtory.spouts.FileSpout
import spray.json._

import java.io.File
import scala.collection.mutable.HashMap
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._


object TempComparitor extends App {
  val testDataCC = groupConnectedComponentsData(new File("/Users/bensteer/github/output/ConnectedComponents_1634144185822"))

  val standardDataCC = HashMap[TimeParams, ConnectedComponentsResults]() ++=
    scala.io.Source.fromFile("src/test/scala/com/raphtory/data/allcommands/connectedcomponents.json").getLines()
      .map(line => line.parseJson.convertTo[ConnectedComponentsResults])
      .map(state => (TimeParams(state.time, state.windowsize), state))

  val correctResultsCC = testDataCC.map(row => row._2.compareTo(standardDataCC(row._1))).fold(true) { (x, y) => x && y }
  println(correctResultsCC)

  val testDataGS = groupGraphStateData(new File("/Users/bensteer/github/output/GraphState_1634144087680"))

  val standardDataGS = HashMap[TimeParams, StateCheckResult]() ++=
    scala.io.Source.fromFile("src/test/scala/com/raphtory/data/allcommands/statetest.json").getLines()
      .map(line => line.parseJson.convertTo[StateCheckResult])
      .map(state => (TimeParams(state.time, state.windowsize), state))

  val correctResultsGS = testDataGS.map(row => row._2.compareTo(standardDataGS(row._1))).fold(true) { (x, y) => x && y }
  println(correctResultsGS)




  def groupGraphStateData(folder: File) = {
   val x =  (for (fileEntry <- folder.listFiles()) yield scala.io.Source.fromFile(fileEntry).getLines().toArray)
      .flatten
      .groupBy(vertex => {
        val line = vertex.split(",")
        (line(0),line(1))
      })
      .map{
        case ((timestamp,window),lines) =>
          val result = lines.map(_.split(",").drop(2).map(_.toLong))
            .map(line=> StateCheckResult(timestamp.toLong,window.toLong, viewTime = 0, vertices = 1, totalInEdges = line(0), totalOutEdges = line(1), vdeletionstotal = line(3), vcreationstotal = line(4), outedgedeletionstotal = line(5), outedgecreationstotal = line(6), inedgedeletionstotal = line(7), inedgecreationstotal = line(8), properties = line(9), propertyhistory = line(10), outedgeProperties = line(11), outedgePropertyHistory = line(12), inedgeProperties = line(13), inedgePropertyHistory = line(14)))
            .fold(StateCheckResult(timestamp.toLong,window.toLong,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0))(combine)
          (TimeParams(timestamp.toLong,window.toLong),result)
      }
    println(x)
    x
  }

  def combine(s1: StateCheckResult,s2: StateCheckResult):StateCheckResult = {
    StateCheckResult(s1.time,s1.windowsize,s1.viewTime,
      s1.vertices + s2.vertices ,
      s1.totalInEdges+s2.totalInEdges ,
      s1.totalOutEdges+s2.totalOutEdges ,
      s1.vdeletionstotal+s2.vdeletionstotal ,
      s1.vcreationstotal+s2.vcreationstotal ,
      s1.outedgedeletionstotal+s2.outedgedeletionstotal ,
      s1.outedgecreationstotal+s2.outedgecreationstotal ,
      s1.inedgedeletionstotal+s2.inedgedeletionstotal ,
      s1.inedgecreationstotal+s2.inedgecreationstotal ,
      s1.properties+s2.properties ,
      s1.propertyhistory+s2.propertyhistory ,
      s1.outedgeProperties+s2.outedgeProperties ,
      s1.outedgePropertyHistory+s2.outedgePropertyHistory ,
      s1.inedgeProperties+s2.inedgeProperties ,
      s1.inedgePropertyHistory+s2.inedgePropertyHistory)
  }


  def groupConnectedComponentsData(folder: File) = {
   (for (fileEntry <- folder.listFiles()) yield scala.io.Source.fromFile(fileEntry).getLines().toArray)
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
  }

}
