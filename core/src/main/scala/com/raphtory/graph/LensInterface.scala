package com.raphtory.graph

import com.raphtory.api.graphstate.GraphState
import com.raphtory.api.table.Row
import com.raphtory.api.visitor.PropertyMergeStrategy.PropertyMerge
import com.raphtory.api.visitor.InterlayerEdge
import com.raphtory.api.visitor.Vertex
import com.raphtory.components.querymanager.GenericVertexMessage
import com.raphtory.storage.pojograph.messaging.VertexMessageHandler

/** Abstract interface for the GraphLens, responsible for executing algorithms
  * @note DoNotDocument
  */
trait LensInterface {

  def partitionID(): Int

  def getFullGraphSize: Int
  def setFullGraphSize(size: Int): Unit

  def executeSelect(f: _ => Row)(onComplete: => Unit): Unit

  def executeSelect(
      f: (_, GraphState) => Row,
      graphState: GraphState
  )(onComplete: => Unit): Unit
  def executeSelect(f: GraphState => Row, graphState: GraphState)(onComplete: => Unit): Unit
  def explodeSelect(f: _ => IterableOnce[Row])(onComplete: => Unit): Unit
  def filteredTable(f: Row => Boolean)(onComplete: => Unit): Unit
  def explodeTable(f: Row => IterableOnce[Row])(onComplete: => Unit): Unit
  def writeDataTable(f: Row => Unit)(onComplete: => Unit): Unit

  def explodeView(
      interlayerEdgeBuilder: Option[Vertex => Seq[InterlayerEdge]]
  )(onComplete: => Unit): Unit

  def reduceView(
      defaultMergeStrategy: Option[PropertyMerge[_, _]],
      mergeStrategyMap: Option[Map[String, PropertyMerge[_, _]]],
      aggregate: Boolean
  )(onComplete: => Unit): Unit

  def runGraphFunction(f: _ => Unit)(onComplete: => Unit): Unit

  def runGraphFunction(
      f: (_, GraphState) => Unit,
      graphState: GraphState
  )(onComplete: => Unit): Unit
  def runMessagedGraphFunction(f: _ => Unit)(onComplete: => Unit): Unit

  def runMessagedGraphFunction(
      f: (_, GraphState) => Unit,
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
