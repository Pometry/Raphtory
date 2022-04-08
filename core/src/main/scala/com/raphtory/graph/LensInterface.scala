package com.raphtory.graph

import com.raphtory.algorithms.api.GraphState
import com.raphtory.algorithms.api.GraphStateImplementation
import com.raphtory.algorithms.api.Row
import com.raphtory.components.querymanager.GenericVertexMessage
import com.raphtory.components.querymanager.VertexMessage
import com.raphtory.graph.visitor.InterlayerEdge
import com.raphtory.graph.visitor.PropertyMergeStrategy.PropertyMerge
import com.raphtory.graph.visitor.Vertex
import com.raphtory.storage.pojograph.messaging.VertexMessageHandler

/** @DoNotDocument
  * Abstract interface for the GraphLens
  *
  * The GraphLens is responsible for executing algorithms
  */
trait LensInterface {

  def getFullGraphSize: Int
  def setFullGraphSize(size: Int): Unit

  def executeSelect(f: Vertex => Row): Unit

  def executeSelect(
      f: (Vertex, GraphState) => Row,
      graphState: GraphState
  ): Unit
  def executeSelect(f: GraphState => Row, graphState: GraphState): Unit
  def explodeSelect(f: Vertex => List[Row]): Unit
  def filteredTable(f: Row => Boolean): Unit
  def explodeTable(f: Row => List[Row]): Unit
  def getDataTable(): List[Row]

  def explodeView(
      interlayerEdgeBuilder: Option[Vertex => Seq[InterlayerEdge]]
  ): Unit

  def reduceView(
      defaultMergeStrategy: Option[PropertyMerge[Any, Any]],
      mergeStrategyMap: Option[Map[String, PropertyMerge[Any, Any]]],
      aggregate: Boolean
  ): Unit

  def runGraphFunction(f: Vertex => Unit): Unit

  def runGraphFunction(
      f: (Vertex, GraphState) => Unit,
      graphState: GraphState
  ): Unit
  def runMessagedGraphFunction(f: Vertex => Unit): Unit

  def runMessagedGraphFunction(
      f: (Vertex, GraphState) => Unit,
      graphState: GraphState
  ): Unit
  def getMessageHandler(): VertexMessageHandler
  def checkVotes(): Boolean
  def sendMessage(msg: GenericVertexMessage[_]): Unit
  def vertexVoted(): Unit
  def nextStep(): Unit
  def receiveMessage(msg: GenericVertexMessage[_]): Unit

  def clearMessages(): Unit

  def getStart(): Long
  def getEnd(): Long
}
