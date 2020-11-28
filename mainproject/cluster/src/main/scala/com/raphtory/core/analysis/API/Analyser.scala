package com.raphtory.core.analysis.API

import akka.actor.ActorContext
import com.raphtory.core.analysis.API.GraphLenses.GraphLens

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox
case class ManagerCount(count: Int)
case class WorkerID(ID: Int)

class BlankAnalyser(args:Array[String]) extends Analyser(args) {
  override def analyse(): Unit = {}
  override def setup(): Unit = {}
  override def returnResults(): Any = {}
  override def defineMaxSteps(): Int = 1
  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {println("howdy!")}
}


case class LoadExternalAnalyser(rawFile: String,args:Array[String]) {
  private val toolbox = currentMirror.mkToolBox()
  private val tree = toolbox.parse(rawFile)
  private val compiledCode = toolbox.compile(tree).apply().asInstanceOf[Class[Analyser]]
  def newAnalyser = compiledCode.getConstructor(classOf[Array[String]]).newInstance(args).asInstanceOf[Analyser]
}

abstract class Analyser(args:Array[String]) extends java.io.Serializable {
  implicit var context: ActorContext      = null
  implicit var managerCount: ManagerCount = null
  implicit var view: GraphLens            = null
  var workerID: Int                       = 0

  private var toPublish:mutable.ArrayBuffer[String] = ArrayBuffer()
  final def sysSetup(context: ActorContext, managerCount: ManagerCount, proxy: GraphLens, ID: Int) = {
    this.context = context
    this.managerCount = managerCount
    this.view = proxy
    this.workerID = ID
  }

  def publishData(data:String) = toPublish +=data
  def getPublishedData() = toPublish.toArray
  def clearPublishedData() =  toPublish = ArrayBuffer()

  def analyse(): Unit
  def setup(): Unit
  def returnResults(): Any

  def defineMaxSteps(): Int
  def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit
  def processWindowResults(results: ArrayBuffer[Any], timestamp: Long, windowSize: Long, viewCompleteTime: Long): Unit =
    processResults(results, timestamp: Long, viewCompleteTime: Long)

}
