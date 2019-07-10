package com.raphtory.core.analysis

import akka.actor.ActorContext
case class ManagerCount(count:Int)
case class Worker(ID:Int)
abstract class Analyser extends java.io.Serializable {
  implicit var context : ActorContext = null
  implicit var managerCount:ManagerCount = null
  implicit var proxy: GraphRepoProxy.type = GraphRepoProxy

  final def sysSetup(context : ActorContext, managerCount : ManagerCount,proxy: GraphRepoProxy.type) = {
    this.context = context
    this.managerCount = managerCount
    this.proxy = proxy
  }

  def analyse()(implicit worker: Worker): Any
  def setup()(implicit workerID: Worker) : Any


}
