package com.raphtory.api.graphview

import com.raphtory.api.algorithm._
import com.raphtory.api.table.Table

/** Core interface for the analysis API.
  *
  * A `GraphView`` is an immutable collection of perspectives over a graph generated for Raphtory that supports
  * all graph operations defined in the [[GraphPerspective]].
  * It implements the operations exposed by [[GraphPerspective]] returning a new `GraphView` or [[MultilayerGraphView]] for
  * those operations that have a graph or multilayer graph as a result.
  * All the operations executed over a `GraphView` get executed individually over every perspective of the graph in the
  * collection. Different perspectives in the collection do not share algorithmic state.
  *
  * @see [[GraphPerspective]], [[TemporalGraph]], [[RaphtoryGraph]], [[TemporalGraphConnection]], [[DeployedTemporalGraph]]
  *
  * @define transformBody @param algorithm Algorithm to apply to the graph
  *                       @return Transformed graph
  *                       @note `transform` keeps track of the name of the applied algorithm and
  *                       clears the message queues at the end of the algorithm
  *
  * @define executeBody @param algorithm Algorithm to run
  *                     @return Table with algorithm results
  *                     @note `execute` keeps track of the name of the applied algorithm
  */
trait GraphView extends GraphPerspective {

  /** Apply a [[Generic]] algorithm to the graph
    *
    * $transformBody
    */
  def transform(algorithm: Generic): Graph

  /** Apply a [[MultilayerProjection]] algorithm to the graph
    *
    * $transformBody
    */
  def transform(algorithm: MultilayerProjection): MultilayerGraph

  /** Apply a [[GenericReduction]] algorithm to the graph
    *
    * $transformBody
    */
  def transform(algorithm: GenericReduction): ReducedGraph

  /** Run a [[GenericallyApplicable]] algorithm on the graph and return results
    *
    * $executeBody
    */
  def execute(algorithm: GenericallyApplicable): Table
}

/** Extends [[GraphView]] with variants of the `transform` and `execute` methods specific to multilayer graphs
  *
  * @see [[GraphView]], [[MultilayerGraphPerspective]], [[MultilayerRaphtoryGraph]], [[MultilayerTemporalGraph]]
  */
trait MultilayerGraphView extends MultilayerGraphPerspective with GraphView {

  /** Apply a `Multilayer` algorithm to the graph
    *
    * $transformBody
    */
  def transform(algorithm: Multilayer): Graph

  /** Apply a `MultilayerReduction` algorithm to the graph
    *
    * $transformBody
    */
  def transform(algorithm: MultilayerReduction): ReducedGraph

  /** Run a `Multilayer` algorithm on the graph and return results
    *
    * $executeBody
    */
  def execute(algorithm: Multilayer): Table

  /** Run a `MultilayerReduction` algorithm on the graph and return results
    *
    * $executeBody
    */
  def execute(algorithm: MultilayerReduction): Table
}
