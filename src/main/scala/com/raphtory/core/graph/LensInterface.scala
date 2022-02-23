package com.raphtory.core.graph

import com.raphtory.core.algorithm.GraphState
import com.raphtory.core.algorithm.Row
import com.raphtory.core.components.querymanager.VertexMessage
import com.raphtory.core.graph.visitor.Vertex
import com.raphtory.core.storage.pojograph.messaging.VertexMessageHandler

trait LensInterface {

  def getFullGraphSize: Int
  def setFullGraphSize(size: Int): Unit

  def executeSelect(f: Vertex => Row): Unit
  def executeSelect(f: (Vertex, GraphState) => Row, graphState: GraphState): Unit
  def executeSelect(f: GraphState => Row, graphState: GraphState): Unit
  def filteredTable(f: Row => Boolean): Unit
  def explodeTable(f: Row => List[Row]): Unit
  def getDataTable(): List[Row]
  def runGraphFunction(f: Vertex => Unit): Unit
  def runGraphFunction(f: (Vertex, GraphState) => Unit, graphState: GraphState): Unit
  def runMessagedGraphFunction(f: Vertex => Unit): Unit
  def runMessagedGraphFunction(f: (Vertex, GraphState) => Unit, graphState: GraphState): Unit
  def getMessageHandler(): VertexMessageHandler
  def checkVotes(): Boolean
  def sendMessage[T](msg: VertexMessage[T]): Unit
  def vertexVoted(): Unit
  def nextStep(): Unit
  def receiveMessage[T](msg: VertexMessage[T]): Unit

  def clearMessages(): Unit

  def getWindow(): Option[Long]
  def getTimestamp(): Long
}
