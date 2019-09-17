package com.raphtory.core.analysis.API

import akka.actor.ActorContext
import com.raphtory.core.analysis.API.GraphRepositoryProxies.LiveProxy

import scala.collection.mutable.ArrayBuffer
case class ManagerCount(count:Int)
case class WorkerID(ID:Int)
abstract class Analyser extends java.io.Serializable {
  implicit var context : ActorContext = null
  implicit var managerCount:ManagerCount = null
  implicit var proxy: LiveProxy = null
  var workerID:Int = 0

  final def sysSetup(context : ActorContext, managerCount : ManagerCount,proxy: LiveProxy,ID:Int) = {
    this.context = context
    this.managerCount = managerCount
    this.proxy = proxy
    this.workerID = ID
  }

  def analyse(): Any
  def setup()  : Any

  def defineMaxSteps() : Int
  def processResults(results:ArrayBuffer[Any],oldResults:ArrayBuffer[Any],viewCompleteTime:Long) : Unit
  def processViewResults(results:ArrayBuffer[Any],oldResults:ArrayBuffer[Any],timestamp:Long,viewCompleteTime:Long) : Unit = processResults(results,oldResults,viewCompleteTime:Long)
  def processWindowResults(results:ArrayBuffer[Any],oldResults:ArrayBuffer[Any],timestamp:Long,windowSize:Long,viewCompleteTime:Long) : Unit = processResults(results,oldResults,viewCompleteTime:Long)
  def processBatchWindowResults(results:ArrayBuffer[Any],oldResults:ArrayBuffer[Any],timestamp:Long,windowSet:Array[Long],viewCompleteTime:Long): Unit = processResults(results,oldResults,viewCompleteTime:Long)
  def checkProcessEnd(results:ArrayBuffer[Any],oldResults:ArrayBuffer[Any]) : Boolean = false
}
