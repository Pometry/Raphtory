package com.raphtory.internals.storage.pojograph.entities.external.edge

import com.raphtory.api.analysis.visitor.IndexedValue
import com.raphtory.api.analysis.visitor.TimePoint
import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.raphtory.internals.storage.pojograph.entities.external.PojoExEntity
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEdge
import com.raphtory.utils.OrderingFunctions._

import scala.reflect.ClassTag

private[pojograph] class PojoExEdge(
    val edge: PojoEdge,
    override val ID: Long,
    override val view: PojoGraphLens,
    override val start: IndexedValue,
    override val end: IndexedValue
) extends PojoExEntity(edge, view, start, end)
        with PojoExReducedEdgeImplementation[PojoExEdge]
        with PojoExDirectedEdgeBase[PojoExEdge, Long] {
  override type Eundir = PojoExReducedEdgeBase

  def this(entity: PojoEdge, id: Long, view: PojoGraphLens) = {
    this(entity, id, view, TimePoint.first(view.start), TimePoint.last(view.end))
  }

  override type ExplodedEdge = PojoExplodedEdge

  def src: Long = edge.getSrcId

  def dst: Long = edge.getDstId

  private var computationValues: Map[String, Any] =
    Map.empty //Partial results kept between supersteps in calculation

  def setState(key: String, value: Any): Unit =
    computationValues += ((key, value))

  def getState[T](key: String, includeProperties: Boolean = false): T =
    if (computationValues.contains(key))
      computationValues(key).asInstanceOf[T]
    else if (includeProperties && properties.contains(key))
      getProperty[T](key).get
    else if (includeProperties)
      throw new Exception(
        s"$key not found within analytical state or properties for edge ${(src,dst)}"
      )
    else
      throw new Exception(s"$key not found within analytical state for edge ${(src,dst)}")

  def getStateOrElse[T](key: String, value: T, includeProperties: Boolean = false): T =
    if (computationValues contains key)
      computationValues(key).asInstanceOf[T]
    else if (includeProperties && properties.contains(key))
      getProperty[T](key).get
    else
      value

  def containsState(key: String, includeProperties: Boolean = false): Boolean =
    computationValues.contains(key) || (includeProperties && properties.contains(key))

  def getOrSetState[T](key: String, value: T, includeProperties: Boolean = false): T = {
    var output_value = value
    if (containsState(key))
      output_value = getState[T](key)
    else {
      if (includeProperties && properties.contains(key))
        output_value = getProperty[T](key).get
      setState(key, output_value)
    }
    output_value
  }

  def appendToState[T: ClassTag](key: String, value: T): Unit = //write function later
    computationValues.get(key) match {
      case Some(arr) =>
        setState(key, arr.asInstanceOf[Array[T]] :+ value)
      case None      =>
        setState(key, Array(value))
    }

  override def explode(): List[ExplodedEdge]                 =
    historyView.collect { case event if event.event => PojoExplodedEdge.fromEdge(this, event) }.toList

  def viewBetween(after: IndexedValue, before: IndexedValue) =
    new PojoExEdge(edge, ID, view, max(after, start), min(before, end))

  override def reversed: PojoExReversedEdge =
    PojoExReversedEdge.fromEdge(this)

  override def combineUndirected(other: PojoExEdge, asInEdge: Boolean): PojoExInOutEdge =
    if (isIncoming)
      new PojoExInOutEdge(this, other, asInEdge)
    else
      new PojoExInOutEdge(other, this, asInEdge)
}

private[pojograph] class PojoExInOutEdge(
    in: PojoExEdge,
    out: PojoExEdge,
    asInEdge: Boolean = false
) extends PojoExInOutEdgeBase[PojoExInOutEdge, PojoExEdge, Long](in, out, asInEdge)
        with PojoExReducedEdgeImplementation[PojoExInOutEdge] {

  private var computationValues: Map[String, Any] =
    Map.empty //Partial results kept between supersteps in calculation



  override def viewBetween(after: IndexedValue, before: IndexedValue): PojoExInOutEdge =
    new PojoExInOutEdge(in.viewBetween(after, before), out.viewBetween(after, before), asInEdge)

  /** concrete type for exploded edge views of this edge which implements
    * [[ExplodedEdge]] with same `IDType`
    */
  override type ExplodedEdge = PojoExplodedEdgeBase[Long]

  /** Return an [[ExplodedEdge]] instance for each time the edge is
    * active in the current view.
    */
  override def explode(): List[ExplodedEdge] = {
    val inEdges         = in.explode().iterator
    val outEdges        = out.explode().iterator
    val transformInEdge =
      if (asInEdge) (edge: PojoExplodedEdge) => edge
      else (edge: PojoExplodedEdge) => edge.reversed

    val transformOutEdge =
      if (asInEdge) (edge: PojoExplodedEdge) => edge.reversed
      else (edge: PojoExplodedEdge) => edge

    val mergedIter: Iterator[PojoExplodedEdgeBase[Long]] = new Iterator[ExplodedEdge] {
      var inEdge: Option[PojoExplodedEdge]  = inEdges.nextOption()
      var outEdge: Option[PojoExplodedEdge] = outEdges.nextOption()

      override def hasNext: Boolean = inEdge.isDefined || outEdge.isDefined

      override def next(): ExplodedEdge =
        if (hasNext)
          inEdge match {
            case Some(ie) =>
              outEdge match {
                case Some(oe) =>
                  if (ie.timePoint < oe.timePoint) {
                    inEdge = inEdges.nextOption()
                    transformInEdge(ie)
                  }
                  else if (ie.timePoint > oe.timePoint) {
                    outEdge = outEdges.nextOption()
                    transformOutEdge(oe)
                  }
                  else {
                    inEdge = inEdges.nextOption()
                    outEdge = outEdges.nextOption()
                    new PojoExplodedInOutEdge(ie, oe, asInEdge)
                  }
                case None     =>
                  inEdge = inEdges.nextOption()
                  transformInEdge(ie)
              }
            case None     =>
              val oe = outEdge.get
              outEdge = outEdges.nextOption()
              transformOutEdge(oe)
          }
        else
          throw new NoSuchElementException
    }
    mergedIter.toList
  }

  override def setState(key: String, value: Any): Unit = ???

  /** Retrieve value from algorithmic state
    *
    * @tparam `T` value type for state
    * @param key               key to use for retrieving state
    * @param includeProperties set this to `true` to fall-through to vertex properties if `key` is not found
    */
  override def getState[T](key: String, includeProperties: Boolean): T = ???

  /** Retrieve value from algorithmic state if it exists or return a default value otherwise
    *
    * @tparam `T` value type for state
    * @param key               key to use for retrieving state
    * @param value             default value to return if state does not exist
    * @param includeProperties set this to `true` to fall-through to vertex properties
    *                          if `key` is not found in algorithmic state
    */
override def getStateOrElse[T](key: String, value: T, includeProperties: Boolean): T = ???

  /** Checks if algorithmic state with key `key` exists
    *
    * @param key               state key to check
    * @param includeProperties Set this to `true` to fall-through to vertex properties if `key` is not found.
    *                          If set, this function only returns `false` if `key` is not included in either algorithmic state
    *                          or vertex properties
    */
  override def containsState(key: String, includeProperties: Boolean): Boolean = ???

  /** Retrieve value from algorithmic state if it exists or set this state to a default value and return otherwise
    *
    * @tparam `T` value type for state
    * @param key               key to use for retrieving state
    * @param value             default value to set and return if state does not exist
    * @param includeProperties set this to `true` to fall-through to vertex properties
    *                          if `key` is not found in algorithmic state. State is only set if this is also not found.
    */
  override def getOrSetState[T](key: String, value: T, includeProperties: Boolean): T = ???

  /** Append new value to existing array or initialise new array if state does not exist
    * The value type of the state is assumed to be `Array[T]` if the state already exists.
    *
    * @tparam `T` value type for state (needs to have a `ClassTag` available due to Scala `Array` implementation)
    * @param key   key to use for retrieving state
    * @param value value to append to state
    */
  override def appendToState[T: ClassTag](key: String, value: T): Unit = ???
}

private[pojograph] class PojoExReversedEdge(
    override val edge: PojoEdge,
    override val ID: Long,
    override val view: PojoGraphLens,
    start: IndexedValue,
    end: IndexedValue
) extends PojoExEdge(edge, ID, view, start, end) {

  def this(entity: PojoEdge, id: Long, view: PojoGraphLens) = {
    this(entity, id, view, TimePoint.first(view.start), TimePoint.last(view.end))
  }

  override def src: Long = edge.getDstId

  override def dst: Long = edge.getSrcId

  override def explode(): List[ExplodedEdge]                          =
    historyView.collect { case event if event.event => PojoReversedExplodedEdge.fromReversedEdge(this, event) }.toList

  override def viewBetween(after: IndexedValue, before: IndexedValue) =
    new PojoExReversedEdge(edge, ID, view, max(after, start), min(before, end))

}

private[pojograph] object PojoExReversedEdge {

  def fromEdge(pojoExEdge: PojoExEdge): PojoExReversedEdge =
    fromEdge(pojoExEdge, pojoExEdge.start, pojoExEdge.end)

  def fromEdge(pojoExEdge: PojoExEdge, start: IndexedValue, end: IndexedValue): PojoExReversedEdge = {
    val id = if (pojoExEdge.ID == pojoExEdge.src) pojoExEdge.dst else pojoExEdge.src
    new PojoExReversedEdge(
            pojoExEdge.edge,
            id,
            pojoExEdge.view,
            max(pojoExEdge.start, start),
            min(pojoExEdge.end, end)
    )
  }
}
