package com.raphtory.internals.graph

import com.raphtory.api.analysis.graphstate.GraphState
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.visitor.InterlayerEdge
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.api.analysis.visitor.PropertyMergeStrategy.PropertyMerge
import com.raphtory.internals.components.querymanager.GenericVertexMessage

/** Abstract interface for the GraphLens, responsible for executing algorithms
  */
private[raphtory] trait LensInterface {

  def partitionID(): Int
  def localNodeCount: Int
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

  def viewUndirected()(onComplete: => Unit): Unit

  def viewDirected()(onComplete: => Unit): Unit

  def viewReversed()(onComplete: => Unit): Unit

  def reduceView(
      defaultMergeStrategy: Option[PropertyMerge[_, _]],
      mergeStrategyMap: Option[Map[String, PropertyMerge[_, _]]],
      aggregate: Boolean
  )(onComplete: => Unit): Unit

  def runGraphFunction(f: Vertex => Unit)(onComplete: => Unit): Unit

  def runGraphFunction(
      f: (_, GraphState) => Unit,
      graphState: GraphState
  )(onComplete: => Unit): Unit
  def runMessagedGraphFunction(f: _ => Unit)(onComplete: => Unit): Unit

  def runMessagedGraphFunction(
      f: (_, GraphState) => Unit,
      graphState: GraphState
  )(onComplete: => Unit): Unit
  def checkVotes(): Boolean
  def sendMessage(msg: GenericVertexMessage[_]): Unit
  def vertexVoted(): Unit
  def nextStep(): Unit
  def receiveMessage(msg: GenericVertexMessage[_]): Unit

  def clearMessages(): Unit

  def start: Long
  def end: Long
  def jobId: String
}
