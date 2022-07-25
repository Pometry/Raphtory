package com.raphtory.internals.management.python

import scala.collection.mutable
import cats.Id
import com.raphtory.api.input.GraphBuilder
import com.raphtory.internals.components.partition.BatchWriter
import com.raphtory.internals.graph.GraphAlteration.EdgeAdd
import com.raphtory.internals.graph.GraphAlteration.GraphUpdate
import com.raphtory.internals.graph.GraphAlteration.VertexAdd
import com.raphtory.internals.management.PyRef
import com.raphtory.internals.management.PythonEncoder

class UnsafeGraphBuilder[T](val ref: PyRef, py: EmbeddedPython[Id])(implicit PE: PythonEncoder[T])
        extends GraphBuilder[T] {

  /** Processes raw data message `tuple` from the spout to extract source node, destination node,
    * timestamp info, etc.
    *
    * A concrete implementation of a `GraphBuilder` needs to override this method to
    * define the graph updates, calling the `addVertex`/`deleteVertex` and `addEdge`/`deleteEdge`
    * methods documented below.
    *
    * @param tuple raw input data
    */
  override def parseTuple(tuple: T): Unit = {
    py.invoke(ref, "parse_tuple", Vector(PE.encode(tuple)))
    val actions = py.eval[Vector[GraphUpdate]](s"${ref.name}.get_actions()")
    py.invoke(ref, "reset_actions", Vector.empty)
    actions.collect {
      case m: VertexAdd =>
        handleVertexAdd(m)
        updateVertexAddStats()
      case m: EdgeAdd   =>
        handleEdgeAdd(m)
        updateEdgeAddStats()
    }
  }

}

case class PythonGraphBuilder[T](pyScript: String, pyClass: String)(implicit PE: PythonEncoder[T])
        extends GraphBuilder[T]
        with Serializable {

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
  override def parseTuple(tuple: T): Unit =
    proxy.parseTuple(tuple)

  override def setBuilderMetaData(builderID: Int, deploymentID: String): Unit =
    proxy.setBuilderMetaData(builderID, deploymentID)

  override private[raphtory] def setupBatchIngestion(
      IDs: mutable.Set[Int],
      writers: mutable.Map[Int, BatchWriter[T]],
      partitions: Int
  ): Unit = proxy.setupBatchIngestion(IDs, writers, partitions)

  override private[raphtory] def getUpdates(tuple: T)(failOnError: Boolean) = proxy.getUpdates(tuple)(failOnError)
}
