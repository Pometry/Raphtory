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

  def executeSelect(f: Vertex => Row)(onComplete: => Unit): Unit

  def executeSelect(
      f: (Vertex, GraphState) => Row,
      graphState: GraphState
  )(onComplete: => Unit): Unit
  def executeSelect(f: GraphState => Row, graphState: GraphState)(onComplete: => Unit): Unit
  def explodeSelect(f: Vertex => List[Row])(onComplete: => Unit): Unit
  def filteredTable(f: Row => Boolean)(onComplete: => Unit): Unit
  def explodeTable(f: Row => List[Row])(onComplete: => Unit): Unit
  def getDataTable(): List[Row]

  def explodeView(
      interlayerEdgeBuilder: Option[Vertex => Seq[InterlayerEdge]]
  )(onComplete: => Unit): Unit

  def reduceView(
      defaultMergeStrategy: Option[PropertyMerge[_, _]],
      mergeStrategyMap: Option[Map[String, PropertyMerge[_, _]]],
      aggregate: Boolean
  )(onComplete: => Unit): Unit

  def runGraphFunction(f: Vertex => Unit)(onComplete: => Unit): Unit

  def runGraphFunction(
      f: (Vertex, GraphState) => Unit,
      graphState: GraphState
  )(onComplete: => Unit): Unit
  def runMessagedGraphFunction(f: Vertex => Unit)(onComplete: => Unit): Unit

  def runMessagedGraphFunction(
      f: (Vertex, GraphState) => Unit,
      graphState: GraphState
  )(onComplete: => Unit): Unit
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
