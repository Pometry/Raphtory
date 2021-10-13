package com.raphtory.algorithms.old

import com.raphtory.core.implementations.objectgraph.ObjectGraphLens
import com.raphtory.core.implementations.objectgraph.messaging.VertexMessageHandler

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


abstract class Analyser[T<:Any](args:Array[String]) extends java.io.Serializable {
  implicit var view: ObjectGraphLens                = null
  implicit var messageHandler: VertexMessageHandler = null

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
