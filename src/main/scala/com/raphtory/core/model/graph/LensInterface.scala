package com.raphtory.core.model.graph

import com.raphtory.core.implementations.generic.messaging.VertexMessageHandler
import com.raphtory.core.model.algorithm.Row
import com.raphtory.core.model.graph.visitor.Vertex

trait LensInterface {

  def getFullGraphSize:Int
  def setFullGraphSize(size: Int):Unit

  def executeSelect(f: Vertex => Row): Unit
  def filteredTable(f: Row => Boolean): Unit
  def explodeTable(f: Row => List[Row]): Unit
  def getDataTable(): List[Row]
  def runGraphFunction(f: Vertex => Unit): Unit
  def runMessagedGraphFunction(f: Vertex => Unit): Unit
  def getMessageHandler(): VertexMessageHandler
  def checkVotes(): Boolean
  def sendMessage(msg: VertexMessage):Unit
  def vertexVoted(): Unit
  def nextStep(): Unit
  def receiveMessage(msg: VertexMessage): Unit

  def getWindow():Option[Long]
  def getTimestamp():Long
}
