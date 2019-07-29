package com.raphtory.core.analysis

import akka.actor.ActorContext
case class ManagerCount(count:Int)
case class WorkerID(ID:Int)
abstract class Analyser extends java.io.Serializable {
  implicit var context : ActorContext = null
  implicit var managerCount:ManagerCount = null
  implicit var proxy: GraphRepoProxy = null

  final def sysSetup(context : ActorContext, managerCount : ManagerCount,proxy: GraphRepoProxy) = {
    this.context = context
    this.managerCount = managerCount
    this.proxy = proxy
  }

  def analyse()(implicit worker: WorkerID): Any
  def setup()(implicit workerID: WorkerID) : Any


}
