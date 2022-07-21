package com.raphtory.api.analysis.graphview

import com.raphtory.api.analysis.algorithm.BaseAlgorithm
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.algorithm.GenericReduction
import com.raphtory.api.analysis.algorithm.GenericallyApplicable
import com.raphtory.api.analysis.algorithm.Multilayer
import com.raphtory.api.analysis.algorithm.MultilayerProjection
import com.raphtory.api.analysis.algorithm.MultilayerReduction
import com.raphtory.api.analysis.graphstate.Accumulator
import com.raphtory.api.analysis.graphstate.GraphState
import com.raphtory.api.analysis.graphstate.GraphStateImplementation
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table
import com.raphtory.api.analysis.table.TableImplementation
import com.raphtory.api.analysis.visitor
import com.raphtory.api.analysis.visitor.Edge
import com.raphtory.api.analysis.visitor.ExplodedVertex
import com.raphtory.api.analysis.visitor.InterlayerEdge
import com.raphtory.api.analysis.visitor.ReducedVertex
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.api.analysis.visitor.PropertyMergeStrategy.PropertyMerge
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.components.querymanager.QueryManagement
import com.raphtory.internals.management.QuerySender

import scala.jdk.CollectionConverters.ListHasAsScala

sealed private[raphtory] trait GraphFunction                             extends QueryManagement
final private[raphtory] case class SetGlobalState(f: GraphState => Unit) extends GraphFunction

sealed private[raphtory] trait GlobalGraphFunction extends GraphFunction

sealed private[raphtory] trait TabularisingGraphFunction extends GraphFunction

final private[raphtory] case class MultilayerView(
    interlayerEdgeBuilder: Option[Vertex => Seq[InterlayerEdge]]
) extends GraphFunction

final private[raphtory] case class ReduceView(
    defaultMergeStrategy: Option[PropertyMerge[_, _]],
    mergeStrategyMap: Option[Map[String, PropertyMerge[_, _]]],
    aggregate: Boolean = false
) extends GraphFunction

final private[raphtory] case class DirectedView() extends GraphFunction

final private[raphtory] case class UndirectedView() extends GraphFunction

final private[raphtory] case class ReversedView() extends GraphFunction

final private[raphtory] case class Step[V <: Vertex](f: (V) => Unit) extends GraphFunction

final private[raphtory] case class StepWithGraph(
    f: (_, GraphState) => Unit
) extends GlobalGraphFunction

final private[raphtory] case class Iterate[V <: Vertex](
    f: V => Unit,
    iterations: Int,
    executeMessagedOnly: Boolean
) extends GraphFunction

final private[raphtory] case class PythonStep(pyObj: Array[Byte]) extends GraphFunction

final private[raphtory] case class PythonStepWithGraph(pyObj: Array[Byte]) extends GlobalGraphFunction

final private[raphtory] case class PythonIterate(
    bytes: Array[Byte],
    iterations: Long,
    executeMessagedOnly: Boolean
) extends GraphFunction

final private[raphtory] case class PythonSetGlobalState(pyObj: Array[Byte]) extends GraphFunction

final private[raphtory] case class IterateWithGraph[V <: Vertex](
    f: (V, GraphState) => Unit,
    iterations: Int,
    executeMessagedOnly: Boolean
) extends GlobalGraphFunction

final private[raphtory] case class Select(f: _ => Row) extends TabularisingGraphFunction

final private[raphtory] case class SelectWithGraph(
    f: (_, GraphState) => Row
) extends TabularisingGraphFunction
        with GlobalGraphFunction

final private[raphtory] case class GlobalSelect(
    f: GraphState => Row
)                                                                   extends TabularisingGraphFunction
        with GlobalGraphFunction
final private[raphtory] case class ExplodeSelect(f: _ => List[Row]) extends TabularisingGraphFunction
final private[raphtory] case class ClearChain()                     extends GraphFunction
final private[raphtory] case class PerspectiveDone()                extends GraphFunction

private[api] trait GraphViewImplementation[
    V <: Vertex,
    G <: GraphViewImplementation[V, G, RG, MG],
    RG <: ReducedGraphViewImplementation[RG, MG],
    MG <: MultilayerGraphViewImplementation[MG, RG]
] extends ConcreteGraphPerspective[V, G, RG, MG]
        with PythonSupport
        with GraphBase[G, RG, MG]
        with GraphView { this: G =>

  private[api] val query: Query
  private[api] val querySender: QuerySender

  override def setGlobalState(f: GraphState => Unit): G = addFunction(SetGlobalState(f))

  override def vertexFilter(f: (V) => Boolean): G =
    step(vertex => if (!f(vertex)) vertex.remove())

  override def vertexFilter(f: (V, GraphState) => Boolean): G =
    step((vertex: V, graphState) => if (!f(vertex, graphState)) vertex.remove())

  override def multilayerView: MG =
    addMFunction(MultilayerView(None))

  override def multilayerView(
      interlayerEdgeBuilder: visitor.Vertex => Seq[InterlayerEdge]
  ): MG =
    addMFunction(MultilayerView(Some(interlayerEdgeBuilder)))

  override def reducedView: RG =
    addRFunction(ReduceView(None, None))

  override def reducedView(
      mergeStrategyMap: Map[String, PropertyMerge[_, _]]
  ): RG =
    addRFunction(ReduceView(None, Some(mergeStrategyMap)))

  override def reducedView(mergeStrategy: PropertyMerge[_, _]): RG =
    addRFunction(ReduceView(Some(mergeStrategy), None))

  override def reducedView(
      defaultMergeStrategy: PropertyMerge[_, _],
      mergeStrategyMap: Map[String, PropertyMerge[_, _]]
  ): RG =
    addRFunction(ReduceView(Some(defaultMergeStrategy), Some(mergeStrategyMap)))

  override def aggregate(
      defaultMergeStrategy: PropertyMerge[_, _],
      mergeStrategyMap: Map[String, PropertyMerge[_, _]]
  ): RG =
    addRFunction(ReduceView(Some(defaultMergeStrategy), Some(mergeStrategyMap), aggregate = true))

  override def undirectedView: G = addFunction(UndirectedView())

  override def directedView: G = addFunction(DirectedView())

  override def reversedView: G = addFunction(ReversedView())

  override def edgeFilter(f: Edge => Boolean, pruneNodes: Boolean): G = {
    val filtered = step { vertex =>
      vertex.outEdges
        .foreach(edge => if (!f(edge)) edge.remove())
    }
    if (pruneNodes)
      filtered.vertexFilter((vertex: V) => vertex.degree > 0)
    else filtered
  }

  override def edgeFilter(f: (Edge, GraphState) => Boolean, pruneNodes: Boolean): G = {
    val filtered = step((vertex, graphState) =>
      vertex.outEdges
        .foreach(edge => if (!f(edge, graphState)) edge.remove())
    )
    if (pruneNodes)
      filtered.vertexFilter(vertex => vertex.degree > 0)
    else filtered
  }

  override def step(f: V => Unit): G = addFunction(Step(f))

  override def pythonStep(pickledPyObj: Array[Byte]): G =
    addFunction(PythonStep(pickledPyObj))

  def loadPythonScript(script: String): G =
    newGraph(query.copy(pyScript = Some(script)), querySender)

  override def step(f: (V, GraphState) => Unit): G =
    addFunction(StepWithGraph(f))

  override def pythonStepState(pickledPyObj: Array[Byte]): G =
    addFunction(PythonStepWithGraph(pickledPyObj))

  override def iterate(
      f: (V) => Unit,
      iterations: Int,
      executeMessagedOnly: Boolean
  ): G = addFunction(Iterate(f, iterations, executeMessagedOnly))

  override def pythonIterate(pyObj: Array[Byte], iterations: Long, executeMessagedOnly: Boolean): G =
    addFunction(PythonIterate(pyObj, iterations, executeMessagedOnly))

  override def pythonSelect(columns: Object): Table =
    pythonSelectSupport(columns)

  override def pythonSelectState(columns: Object): Table =
    pythonSelectStateSupport(columns)

  private def pythonSelectStateSupport(columns: Object) = {
    val cs   = columns match {
      case arr: Array[_]           => arr.iterator
      case list: java.util.List[_] => list.asScala.iterator
    }
    val cols = cs.collect { case s: String => s }.toVector
    globalSelect { state =>
      print(state(cols(0)))
      val row: Seq[Any] =
        cols.map { name =>
          val name1: Accumulator[Any, Any] = state[Any, Any](name)
          name1.value
        }
      println(row)
      Row(row: _*)
    }
  }

  private def pythonSelectSupport(columns: Object) = {
    val cs   = columns match {
      case arr: Array[_]           => arr.iterator
      case list: java.util.List[_] => list.asScala.iterator
    }
    val cols = cs.collect { case s: String => s }.toVector
    this.select { vertex =>
      val maybeObjects =
        cols.flatMap(name => Option(vertex.getStateOrElse[Object](name, null, includeProperties = true)))
      val row          = vertex.name() +: maybeObjects
      Row(row: _*)
    }
  }

  override def pythonSetGlobalState(pyObj: Array[Byte]): G =
    addFunction(PythonSetGlobalState(pyObj))

  override def iterate(
      f: (V, GraphState) => Unit,
      iterations: Int,
      executeMessagedOnly: Boolean
  ): G = addFunction(IterateWithGraph(f, iterations, executeMessagedOnly))

  override def select(f: V => Row): Table =
    addSelect(Select(f))

  override def select(f: (V, GraphState) => Row): Table =
    addSelect(SelectWithGraph(f))

  override def globalSelect(f: GraphState => Row): Table =
    addSelect(GlobalSelect(f))

  override def explodeSelect(f: V => List[Row]): Table =
    addSelect(ExplodeSelect(f))

  override def clearMessages(): G =
    addFunction(ClearChain())

  /**  Execute only the apply step of the algorithm on every perspective and returns a new RaphtoryGraph with the result.
    *  @param algorithm algorithm to apply
    */
  def transform(algorithm: Generic): G =
    algorithm
      .apply(withTransformedName(algorithm))
      .clearMessages()

  def transform(algorithm: MultilayerProjection): MG =
    algorithm(withTransformedName(algorithm)).clearMessages()

  def transform(algorithm: GenericReduction): RG =
    algorithm(withTransformedName(algorithm)).clearMessages()

  /** Execute the algorithm on every perspective and returns a new `RaphtoryGraph` with the result.
    * @param algorithm to apply
    */
  def execute(algorithm: GenericallyApplicable): Table =
    algorithm.run(withTransformedName(algorithm))

  private def addFunction(function: GraphFunction) =
    newGraph(query.copy(graphFunctions = query.graphFunctions.enqueue(function)), querySender)

  private def addRFunction(function: GraphFunction) =
    newRGraph(query.copy(graphFunctions = query.graphFunctions.enqueue(function)), querySender)

  private def addMFunction(function: GraphFunction) =
    newMGraph(query.copy(graphFunctions = query.graphFunctions.enqueue(function)), querySender)

  private def addSelect(function: GraphFunction) =
    new TableImplementation(
            query.copy(graphFunctions = query.graphFunctions.enqueue(function)),
            querySender
    )

  private[api] def withTransformedName(algorithm: BaseAlgorithm) = {
    val newName = query.name match {
      case "" => algorithm.name
      case _  => query.name + ":" + algorithm.name
    }
    newGraph(query.copy(name = newName, _bootstrap = query._bootstrap + algorithm.getClass), querySender)
  }
}

private[api] trait MultilayerGraphViewImplementation[
    G <: MultilayerGraphViewImplementation[G, RG],
    RG <: ReducedGraphViewImplementation[RG, G]
] extends GraphViewImplementation[ExplodedVertex, G, RG, G]
        with ConcreteMultilayerGraphPerspective[G, RG]
        with MultilayerGraphView { this: G =>

  def transform(algorithm: Multilayer): G =
    algorithm(withTransformedName(algorithm)).clearMessages()

  def transform(algorithm: MultilayerReduction): RG =
    algorithm(withTransformedName(algorithm)).clearMessages()

  def execute(algorithm: Multilayer): Table =
    algorithm.run(withTransformedName(algorithm))

  def execute(algorithm: MultilayerReduction): Table =
    algorithm.run(withTransformedName(algorithm))

  private[api] def newGraph(query: Query, querySender: QuerySender): G =
    newMGraph(query, querySender)
}

private[api] trait ReducedGraphViewImplementation[G <: ReducedGraphViewImplementation[
        G,
        MG
], MG <: MultilayerGraphViewImplementation[MG, G]]
        extends ConcreteReducedGraphPerspective[G, MG]
        with GraphViewImplementation[ReducedVertex, G, G, MG] { this: G =>

  private[api] def newGraph(query: Query, querySender: QuerySender): G =
    newRGraph(query, querySender)
}

private[api] trait FixedGraph[G] {
  private[api] val query: Query
  private[api] val querySender: QuerySender
  private[api] def newGraph(query: Query, querySender: QuerySender): G
}

private[api] trait GraphBase[G, RG, MG] extends FixedGraph[G] {
  private[api] def newRGraph(query: Query, querySender: QuerySender): RG
  private[api] def newMGraph(query: Query, querySender: QuerySender): MG
}
