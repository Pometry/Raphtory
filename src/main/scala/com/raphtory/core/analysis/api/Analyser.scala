package com.raphtory.core.analysis.api

import akka.actor.ActorContext
import com.raphtory.core.analysis.ObjectGraphLens
import com.raphtory.core.model.communication.VertexMessageHandler

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox
case class ManagerCount(count: Int)
case class WorkerID(ID: Int)

case class LoadExternalAnalyser(rawFile: String,args:Array[String]) {
  private val toolbox = currentMirror.mkToolBox()
  private val tree = toolbox.parse(rawFile)
  private val compiledCode = toolbox.compile(tree).apply().asInstanceOf[Class[Analyser[Any]]]
  def newAnalyser: Analyser[Any] = compiledCode.getConstructor(classOf[Array[String]]).newInstance(args)
}

abstract class Analyser[T<:Any](args:Array[String]) extends java.io.Serializable {
  implicit var view: ObjectGraphLens                      = null
  implicit var messageHandler: VertexMessageHandler = null
  var workerID: Int                                 = 0

  private var toPublish:mutable.ArrayBuffer[String] = ArrayBuffer()
  final def sysSetup(proxy: ObjectGraphLens, messageHandler:VertexMessageHandler) = {
    this.view = proxy
    this.messageHandler = messageHandler
  }

  def getArgs():Array[String] = args

  def analyse(): Unit
  def setup(): Unit
  def returnResults(): T

  def defineMaxSteps(): Int
  def extractResults(results: List[T]): Map[String, Any]
 
}
