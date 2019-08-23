package com.raphtory.core.analysis

import akka.actor.ActorContext
import com.raphtory.core.analysis.GraphRepositoryProxies.GraphProxy

import scala.collection.mutable.ArrayBuffer
case class ManagerCount(count:Int)
case class WorkerID(ID:Int)
abstract class Analyser extends java.io.Serializable {
  implicit var context : ActorContext = null
  implicit var managerCount:ManagerCount = null
  implicit var proxy: GraphProxy = null

  final def sysSetup(context : ActorContext, managerCount : ManagerCount,proxy: GraphProxy) = {
    this.context = context
    this.managerCount = managerCount
    this.proxy = proxy
  }

  def analyse()(implicit worker: WorkerID): Any
  def setup()(implicit workerID: WorkerID) : Any

  def defineMaxSteps() : Int
  def processResults(results:ArrayBuffer[Any],oldResults:ArrayBuffer[Any],timestamp:Long,windowSize:Long) : Unit
  def checkProcessEnd(results:ArrayBuffer[Any],oldResults:ArrayBuffer[Any]) : Boolean = false
}
