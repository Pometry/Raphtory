package com.raphtory.core.analysis

import akka.actor.ActorContext
abstract class Analyser extends java.io.Serializable {
  implicit var context : ActorContext = null
  implicit var managerCount : Int = 0
  implicit var workerID: Short = 0
  implicit var proxy: GraphRepoProxy.type = GraphRepoProxy

  final def sysSetup(context : ActorContext, managerCount : Int, workerID:Short,proxy: GraphRepoProxy.type) = {
    this.context = context
    this.managerCount = managerCount
    this.workerID = workerID
    this.proxy = proxy
  }

  def analyse(): Any
  def setup() : Any


}
