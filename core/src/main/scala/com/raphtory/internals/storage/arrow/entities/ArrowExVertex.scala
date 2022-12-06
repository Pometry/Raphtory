package com.raphtory.internals.storage.arrow.entities

import com.raphtory.api.analysis.visitor.ReducedVertex
import com.raphtory.arrowcore.model
import com.raphtory.arrowcore.model.Entity
import com.raphtory.arrowcore.model.{Vertex => ArrVertex}
import com.raphtory.internals.communication.SchemaProviderInstances
import com.raphtory.internals.communication.SchemaProviderInstances._
import com.raphtory.internals.components.querymanager.FilteredInEdgeMessage
import com.raphtory.internals.components.querymanager.FilteredOutEdgeMessage
import com.raphtory.internals.components.querymanager.SchemaProvider
import com.raphtory.internals.components.querymanager.VertexMessage
import com.raphtory.internals.storage.arrow.ArrowEntityStateRepository
import com.raphtory.internals.storage.arrow.RichVertex

import scala.collection.View
import scala.jdk.CollectionConverters.CollectionHasAsScala

class ArrowExVertex(val repo: ArrowEntityStateRepository, val vertex: ArrVertex)
        extends ReducedVertex
        with ArrowExEntity {

  def entity: Entity = vertex

  /** ID type of this vertex */
  override type IDType = Long

  /** Concrete edge type for this vertex which implements [[com.raphtory.api.analysis.visitor.Edge Edge]] */
  override type Edge = ArrowExEdge

  /** implicit ordering object for use when comparing vertex IDs */
  implicit override val IDOrdering: Ordering[Long] = Ordering.Long

  /** Get the ID type of this vertex */
  override def ID: Long = entity.getGlobalId

  /** Check if vertex has received messages */
  override def hasMessage: Boolean = repo.hasMessage(ID)

  /** Queue of received messages
    *
    * @tparam `T` message data type
    */
  override def messageQueue[T]: Seq[T] =
    repo.releaseQueue(ID)

  /** Vote to stop iterating (iteration stops if all vertices voted to halt) */
  override def voteToHalt(): Unit = repo.vertexVoted()

  /** Send data to another vertex at next Step/Iteration
    *
    * @param vertexId Vertex Id of target vertex for the message
    * @param data     message data to send
    */
  override def messageVertex[T](vertexId: IDType, data: T)(implicit provider: SchemaProvider[T]): Unit =
    repo.sendMessage(VertexMessage(repo.superStep + 1, vertexId, data))

  /** Return all edges starting at this vertex
    */
  override def outEdges: View[ArrowExEdge] =
    vertex
      .outgoingEdges(repo.start, repo.end)
      .map(mkArrOutEdge)
      .filter(e => repo.isEdgeAlive(e.src, e.dst))

  private def mkArrOutEdge(e: model.Edge) = {
    val dst =
      if (!e.isDstGlobal) repo.asGlobal(e.getDstVertex)
      else e.getDstVertex
    new ArrowExEdge(dst, e, repo)
  }

  /** Return all edges ending at this vertex
    */
  override def inEdges: View[ArrowExEdge] = {
    vertex.incomingEdges(repo.start, repo.end)
      .map(mkArrInEdge)
      .filter(e => repo.isEdgeAlive(e.src, e.dst))
  }

  private def mkArrInEdge(e: model.Edge) = {
    val src =
      if (!e.isSrcGlobal) repo.asGlobal(e.getSrcVertex)
      else e.getSrcVertex
    new ArrowExEdge(src, e, repo)
  }

  /** Return specified edge if it is an out-edge of this vertex
    *
    * @param id ID of edge to return
    */
  override def getOutEdge(id: Long): Option[Edge] = ???

  /** Return specified edge if it is an in-edge of this vertex
    *
    * @param id ID of edge to return
    */
  override def getInEdge(id: Long): Option[Edge] = ???

  /** Return specified edge if it is an in-edge or an out-edge of this vertex
    *
    * This function returns a list of edges, where the list is empty if neither an in-edge nor an out-edge
    * with this id exists, contains one element if either an in-edge or an out-edge with the id exists, or
    * contains two elements if both in-edge and out-edge exist.
    *
    * @param id ID of edge to return
    */
  override def getEdge(id: Long): View[Edge] = ???

  /** Filter this vertex and remove it and all its edges from the GraphPerspective */
  override def remove(): Unit = {
    repo.removeVertex(ID)
    // all outgoing edges are present here on this node where the vertex is removed from
    outEdges.foreach { edge =>
      val other = if (edge.src == ID) edge.dst else edge.src
      repo.sendMessage(FilteredOutEdgeMessage(repo.superStep, other, ID))
    }
    inEdges.foreach { edge =>
      val other = if (edge.src == ID) edge.dst else edge.src
      repo.sendMessage(FilteredInEdgeMessage(repo.superStep, other, ID))
    }
  }

  /** Remove an entry in the entity's algorithmic state. */
  override def clearState(key: String): Unit = ???

  implicit override val provider: SchemaProvider[Long] = SchemaProviderInstances.longSchemaProvider

  /** Return all edges starting at this vertex
    *
    * @param after  only return edges that are active after time `after`
    * @param before only return edges that are active before time `before`
    *
    *               The `after` and `before` parameters also restrict the history of the returned edges such that it only
    *               contains events within the window.
    */
  override def getOutEdges(after: Long, before: Long): View[ArrowExEdge] =
    vertex.outgoingEdges.map(mkArrOutEdge)

  /** Return all edges ending at this vertex
    *
    * @param after  only return edges that are active after time `after`
    * @param before only return edges that are active before time `before`
    *
    *               The `after` and `before` parameters also restrict the history of the returned edges such that it only
    *               contains events within the window.
    */
  override def getInEdges(after: Long, before: Long): View[ArrowExEdge] =
    vertex.incomingEdges.map(mkArrInEdge)

  /** Return specified edge if it is an out-edge of this vertex
    *
    * @param id     ID of edge to return
    * @param after  only return edge if it is active after time `after`
    * @param before only return edge if it is active before time `before`
    *
    *               The `after` and `before` parameters also restrict the history of the returned edge such that it only
    *               contains events within the window.
    */
  override def getOutEdge(id: Long, after: Long, before: Long): Option[ArrowExEdge] =
    getOutEdges(after, before).find(e => e.dst == id)

  /** Return specified edge if it is an in-edge of this vertex
    *
    * @param id     ID of edge to return
    * @param after  only return edge if it is active after time `after`
    * @param before only return edge if it is active before time `before`
    *
    *               The `after` and `before` parameters also restrict the history of the returned edge such that it only
    *               contains events within the window.
    */
  override def getInEdge(id: Long, after: Long, before: Long): Option[ArrowExEdge] =
    getInEdges(after, before).find(e => e.src == id)

  /** Return a list of keys for available properties for the entity */
  override def getPropertySet(): List[String] = {
    val schema       = vertex.getRaphtory.getPropertySchema
    val versioned    = schema.versionedVertexProperties().asScala.map(_.name())
    val nonVersioned = schema.nonversionedVertexProperties().asScala.map(_.name())
    (versioned ++ nonVersioned).toList
  }
}
