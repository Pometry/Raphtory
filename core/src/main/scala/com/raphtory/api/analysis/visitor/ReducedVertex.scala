package com.raphtory.api.analysis.visitor

/** Vertex with concrete `Long` `IDType` (i.e. a non-exploded Vertex) */
trait ReducedVertex extends Vertex {
  override type IDType = Long
  override type Edge <: ReducedEdge

  /** Concrete type for this vertex's exploded edges which implements
    * [[com.raphtory.api.analysis.visitor.ExplodedEdge ExplodedEdge]]
    */
  type ExplodedEdge = Edge#ExplodedEdge

  /** Return all edges starting or ending at this vertex
    * @param after only return edges that are active after time `after`
    * @param before only return edges that are active before time `before`
    *
    * The `after` and `before` parameters also restrict the history of the returned edges such that it only
    * contains events within the window.
    */
  def getAllEdges(after: Long = Long.MinValue, before: Long = Long.MaxValue): List[Edge] =
    getInEdges(after, before) ++ getOutEdges(after, before)

  /** Return all edges starting at this vertex
    * @param after only return edges that are active after time `after`
    * @param before only return edges that are active before time `before`
    *
    * The `after` and `before` parameters also restrict the history of the returned edges such that it only
    * contains events within the window.
    */
  def getOutEdges(after: Long = Long.MinValue, before: Long = Long.MaxValue): List[Edge]

  /** Return all edges ending at this vertex
    * @param after  only return edges that are active after time `after`
    * @param before only return edges that are active before time `before`
    *
    * The `after` and `before` parameters also restrict the history of the returned edges such that it only
    * contains events within the window.
    */
  def getInEdges(after: Long = Long.MinValue, before: Long = Long.MaxValue): List[Edge]

  /** Return specified edge if it is an out-edge of this vertex
    * @param id ID of edge to return
    * @param after only return edge if it is active after time `after`
    * @param before only return edge if it is active before time `before`
    *
    * The `after` and `before` parameters also restrict the history of the returned edge such that it only
    * contains events within the window.
    */
  def getOutEdge(
      id: IDType,
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Option[Edge]

  /** Return specified edge if it is an in-edge of this vertex
    * @param id ID of edge to return
    * @param after only return edge if it is active after time `after`
    * @param before only return edge if it is active before time `before`
    *
    * The `after` and `before` parameters also restrict the history of the returned edge such that it only
    * contains events within the window.
    */
  def getInEdge(
      id: IDType,
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Option[Edge]

  /** Return specified edge if it is an in-edge or an out-edge of this vertex
    *
    * This function returns a list of edges, where the list is empty if neither an in-edge nor an out-edge
    * with this id exists, contains one element if either an in-edge or an out-edge with the id exists, or
    * contains two elements if both in-edge and out-edge exist.
    *
    * @param id ID of edge to return
    * @param after only return edge if it is active after time `after`
    * @param before only return edge if it is active before time `before`
    *
    * The `after` and `before` parameters also restrict the history of the returned edges such that it only
    * contains events within the window.
    */
  def getEdge(
      id: IDType,
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): List[Edge] = List(getInEdge(id, after, before), getOutEdge(id, after, before)).flatten

  /** Return all exploded [[com.raphtory.api.analysis.visitor.ExplodedEdge ExplodedEdge]] views for each time point
    * that an in- or out-edge of this vertex is active
    *
    * @param after  only return views for activity after time `after`
    * @param before only return view for activity before time `before`
    */
  def explodeAllEdges(
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): List[ExplodedEdge] =
    getAllEdges(after, before).flatMap(_.explode())

  /** Return all exploded [[com.raphtory.api.analysis.visitor.ExplodedEdge ExplodedEdge]] views for each time point
    * that an out-edge of this vertex is active
    *
    * @param after  only return views for activity after time `after`
    * @param before only return view for activity before time `before`
    */
  def explodeOutEdges(
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): List[ExplodedEdge] =
    getOutEdges(after, before).flatMap(_.explode())

  /** Return all exploded [[com.raphtory.api.analysis.visitor.ExplodedEdge ExplodedEdge]] views for each time point
    * that an in-edge of this vertex is active
    *
    * @param after  only return views for activity after time `after`
    * @param before only return view for activity before time `before`
    */
  def explodeInEdges(
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): List[ExplodedEdge] =
    getInEdges(after, before).flatMap(_.explode())

  /** Return an individual exploded [[com.raphtory.api.analysis.visitor.ExplodedEdge ExplodedEdge]] views for an individual edge
    * if it is an out-edge of this vertex
    *
    * @param id ID of edge to explode
    * @param after  only return views for activity after time `after`
    * @param before only return view for activity before time `before`
    */
  def explodeOutEdge(
      id: IDType,
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Option[List[ExplodedEdge]] =
    getOutEdge(id, after, before).map(_.explode())

  /** Return exploded [[com.raphtory.api.analysis.visitor.ExplodedEdge ExplodedEdge]] views for an individual edge
    * if it is an in-edge of this vertex
    *
    * @param id ID of edge to explode
    * @param after  only return views for activity after time `after`
    * @param before only return view for activity before time `before`
    */
  def explodeInEdge(
      id: IDType,
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Option[List[ExplodedEdge]] =
    getInEdge(id, after, before).map(_.explode())

  /** Return an individual exploded [[com.raphtory.api.analysis.visitor.ExplodedEdge ExplodedEdge]] views for an individual edge
    * if it is an in- or out-edge of this vertex
    *
    * @param id ID of edge to explode
    * @param after  only return views for activity after time `after`
    * @param before only return view for activity before time `before`
    */
  def explodedEdge(
      id: IDType,
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Option[List[ExplodedEdge]] = {
    val edges = getEdge(id, after, before)
    if (edges.isEmpty)
      None
    else
      Some(edges.flatMap(_.explode()))
  }
}
