package com.raphtory.core.analysis.API

import akka.actor.ActorContext
import com.raphtory.core.analysis.API.GraphLenses.LiveLens

import scala.collection.mutable.ArrayBuffer
case class ManagerCount(count: Int)
case class WorkerID(ID: Int)
abstract class Analyser extends java.io.Serializable {
  implicit var context: ActorContext      = null
  implicit var managerCount: ManagerCount = null
  implicit var proxy: LiveLens            = null
  var workerID: Int                       = 0
  var args:Array[String]                  = null
  final def sysSetup(context: ActorContext, managerCount: ManagerCount, proxy: LiveLens, ID: Int,args:Array[String]) = {
    this.context = context
    this.managerCount = managerCount
    this.proxy = proxy
    this.workerID = ID
    this.args = args
  }

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
