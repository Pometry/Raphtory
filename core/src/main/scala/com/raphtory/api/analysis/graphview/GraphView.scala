package com.raphtory.api.analysis.graphview

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.algorithm.GenericReduction
import com.raphtory.api.analysis.algorithm.GenericallyApplicable
import com.raphtory.api.analysis.algorithm.Multilayer
import com.raphtory.api.analysis.algorithm.MultilayerProjection
import com.raphtory.api.analysis.algorithm.MultilayerReduction
import com.raphtory.api.analysis.table.Table

/** Core interface for the analysis API.
  *
  * A `GraphView` is an immutable collection of perspectives over a graph generated for Raphtory that supports
  * all graph operations defined in the [[GraphPerspective]].
  * It implements the operations exposed by [[GraphPerspective]] returning a new `GraphView` or [[MultilayerGraphView]] for
  * those operations that have a graph or multilayer graph as a result.
  * All the operations executed over a `GraphView` get executed individually over every perspective of the graph in the
  * collection. Different perspectives in the collection do not share algorithmic state.
  *
  * @see [[GraphPerspective]], [[TemporalGraph]], [[RaphtoryGraph]], [[DeployedTemporalGraph]]
  */
trait GraphView extends GraphPerspective {

  /** Apply a [[com.raphtory.api.analysis.algorithm.Generic Generic]] algorithm to the graph
    *
    * @param algorithm Algorithm to apply to the graph
    * @return Transformed graph
    * @note `transform` keeps track of the name of the applied algorithm and clears the message queues at the end of the algorithm
    */
  def transform(algorithm: Generic): Graph

  /** Apply a [[com.raphtory.api.analysis.algorithm.MultilayerProjection MultilayerProjection]] algorithm to the graph
    *
    * @param algorithm Algorithm to apply to the graph
    * @return Transformed graph
    * @note `transform` keeps track of the name of the applied algorithm and clears the message queues at the end of the algorithm
    */
  def transform(algorithm: MultilayerProjection): MultilayerGraph

  /** Apply a [[com.raphtory.api.analysis.algorithm.GenericReduction GenericReduction]] algorithm to the graph
    *
    * @param algorithm Algorithm to apply to the graph
    * @return Transformed graph
    * @note `transform` keeps track of the name of the applied algorithm and clears the message queues at the end of the algorithm
    */
  def transform(algorithm: GenericReduction): ReducedGraph

  /** Run a [[com.raphtory.api.analysis.algorithm.GenericallyApplicable GenericallyApplicable]] algorithm on the graph and return results
    *
    *  @param algorithm Algorithm to run
    *  @return Table with algorithm results
    *  @note `execute` keeps track of the name of the applied algorithm
    */
  def execute(algorithm: GenericallyApplicable): Table

  def addClass(clazz: Class[_]): Graph

  def addDynamicPath(name: String*): Graph

}

/** Extends [[GraphView]] with variants of the `transform` and `execute` methods specific to multilayer graphs
  *
  * @see [[GraphView]], [[MultilayerGraphPerspective]], [[MultilayerRaphtoryGraph]], [[MultilayerTemporalGraph]]
  */
trait MultilayerGraphView extends MultilayerGraphPerspective with GraphView {

  /** Apply a `Multilayer` algorithm to the graph
    *
    * @param algorithm Algorithm to apply to the graph
    * @return Transformed graph
    * @note `transform` keeps track of the name of the applied algorithm and clears the message queues at the end of the algorithm
    */
  def transform(algorithm: Multilayer): Graph

  /** Apply a `MultilayerReduction` algorithm to the graph
    *
    * @param algorithm Algorithm to apply to the graph
    * @return Transformed graph
    * @note `transform` keeps track of the name of the applied algorithm and clears the message queues at the end of the algorithm
    */
  def transform(algorithm: MultilayerReduction): ReducedGraph

  /** Run a `Multilayer` algorithm on the graph and return results
    *
    *  @param algorithm Algorithm to run
    *  @return Table with algorithm results
    *  @note `execute` keeps track of the name of the applied algorithm
    */
  def execute(algorithm: Multilayer): Table

  /** Run a `MultilayerReduction` algorithm on the graph and return results
    *
    *  @param algorithm Algorithm to run
    *  @return Table with algorithm results
    *  @note `execute` keeps track of the name of the applied algorithm
    */
  def execute(algorithm: MultilayerReduction): Table
}
