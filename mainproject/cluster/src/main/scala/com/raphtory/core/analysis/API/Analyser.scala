package com.raphtory.core.analysis.API

import akka.actor.ActorContext
import com.raphtory.core.analysis.API.GraphLenses.LiveLens

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox
case class ManagerCount(count: Int)
case class WorkerID(ID: Int)

case class LoadExternalAnalyser(rawFile: String,args:Array[String]) {
  private val toolbox = currentMirror.mkToolBox()
  private val tree = toolbox.parse(rawFile)
  private val compiledCode = toolbox.compile(tree).apply().asInstanceOf[Class[Analyser]]
  def newAnalyser = compiledCode.getConstructor(classOf[Array[String]]).newInstance(args).asInstanceOf[Analyser]
}

abstract class Analyser(args:Array[String]) extends java.io.Serializable {
  implicit var context: ActorContext      = null
  implicit var managerCount: ManagerCount = null
  implicit var proxy: LiveLens            = null
  var workerID: Int                       = 0

  private val toPublish:mutable.ArrayBuffer[String] = ArrayBuffer()
  final def sysSetup(context: ActorContext, managerCount: ManagerCount, proxy: LiveLens, ID: Int) = {
    this.context = context
    this.managerCount = managerCount
    this.proxy = proxy
    this.workerID = ID
  }

  def publishData(data:String) = toPublish +=data
  def getPublishedData() = toPublish.toArray
  def analyse(): Unit
  def setup(): Unit
  def returnResults(): Any

  def defineMaxSteps(): Int
  def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit
  def processViewResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit =
    processResults(results, timestamp: Long, viewCompleteTime: Long)
  def processWindowResults(results: ArrayBuffer[Any], timestamp: Long, windowSize: Long, viewCompleteTime: Long): Unit =
    processResults(results, timestamp: Long, viewCompleteTime: Long)
  def processBatchWindowResults(
      results: ArrayBuffer[Any],
      timestamp: Long,
      windowSet: Array[Long],
      viewCompleteTime: Long
  ): Unit = processResults(results, timestamp: Long, viewCompleteTime: Long)
}
