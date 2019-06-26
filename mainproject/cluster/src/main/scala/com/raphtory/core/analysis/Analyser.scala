package com.raphtory.core.analysis

import akka.actor.ActorContext
abstract class Analyser extends java.io.Serializable {
  implicit var context : ActorContext
  implicit var managerCount : Int
  implicit  val workerID: Int

  final def sysSetup()(implicit context : ActorContext, managerCount : Int, workerID:Int) = {
    this.context = context
    this.managerCount = managerCount
  }

  def analyse()(implicit proxy : GraphRepoProxy.type, managerCount : Int) : Any
  def setup()(implicit proxy : GraphRepoProxy.type) : Any


}
