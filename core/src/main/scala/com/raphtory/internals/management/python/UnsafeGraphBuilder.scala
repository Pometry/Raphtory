package com.raphtory.internals.management.python

import scala.collection.mutable
import cats.Id
import com.raphtory.api.input.Graph
import com.raphtory.api.input.GraphBuilder
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.graph.GraphAlteration.EdgeAdd
import com.raphtory.internals.graph.GraphAlteration.GraphUpdate
import com.raphtory.internals.graph.GraphAlteration.VertexAdd
import com.raphtory.internals.management.PyRef
import com.raphtory.internals.management.PythonEncoder

class UnsafeGraphBuilder[T](val ref: PyRef, py: EmbeddedPython[Id])(implicit PE: PythonEncoder[T])
        extends GraphBuilder[T] {

  logger.debug("Started UnsafeGraphBuilder")
  py.invoke(ref, "_set_jvm_builder", Vector(this))
  logger.debug("Registered proxy in Python")

  /** Processes raw data message `tuple` from the spout to extract source node, destination node,
    * timestamp info, etc.
    *
    * A concrete implementation of a `GraphBuilder` needs to override this method to
    * define the graph updates, calling the `addVertex`/`deleteVertex` and `addEdge`/`deleteEdge`
    * methods documented below.
    *
    * @param tuple raw input data
    */
  def parseTuple(tuple: T): Unit =
    py.invoke(ref, "parse_tuple", Vector(PE.encode(tuple)))

  def get_current_index(): Long = 1
  // index

  /** Processes raw data message `tuple` from the spout to extract source node, destination node,
    * timestamp info, etc.
    *
    * A concrete implementation of a `GraphBuilder` needs to override this method to
    * define the graph updates, calling the `addVertex`/`deleteVertex` and `addEdge`/`deleteEdge`
    * methods documented below.
    *
    * @param tuple raw input data
    */
  override def parse(graph: Graph, tuple: T): Unit = {}
}

case class PythonGraphBuilder[T](pyScript: String, pyClass: String)(implicit PE: PythonEncoder[T])
        extends GraphBuilder[T]
        with Serializable {

  logger.debug("Started PythonGraphBuilder")
  @transient lazy val py    = UnsafeEmbeddedPythonProxy(List(pyScript))
  @transient lazy val proxy = py.loadGraphBuilder[T](pyClass, None)

  /** Processes raw data message `tuple` from the spout to extract source node, destination node,
    * timestamp info, etc.
    *
    * A concrete implementation of a `GraphBuilder` needs to override this method to
    * define the graph updates, calling the `addVertex`/`deleteVertex` and `addEdge`/`deleteEdge`
    * methods documented below.
    *
    * @param tuple raw input data
    */
  //override def parseTuple(tuple: T): Unit =
  //proxy.parseTuple(tuple)

  //override def setBuilderMetaData(builderID: Int, deploymentID: String): Unit =
  //  proxy.setBuilderMetaData(builderID, deploymentID)

//  override private[raphtory] def setupBatchIngestion(
//      IDs: mutable.Set[Int],
//      batchWriters: collection.Map[Int, BatchWriter[T]],
//      partitions: Int
//  ): Unit = proxy.setupBatchIngestion(IDs, batchWriters, partitions)

//  override private[raphtory] def setupStreamIngestion(
//      streamWriters: collection.Map[Int, EndPoint[GraphUpdate]]
//  ): Unit = proxy.setupStreamIngestion(streamWriters)

//  override private[raphtory] def sendUpdates(tuple: T, tupleIndex: Long)(failOnError: Boolean): Unit =
//    proxy.sendUpdates(tuple, tupleIndex)(failOnError)

  /** Processes raw data message `tuple` from the spout to extract source node, destination node,
    * timestamp info, etc.
    *
    * A concrete implementation of a `GraphBuilder` needs to override this method to
    * define the graph updates, calling the `addVertex`/`deleteVertex` and `addEdge`/`deleteEdge`
    * methods documented below.
    *
    * @param tuple raw input data
    */
  override def parse(graph: Graph, tuple: T): Unit = {}
}
