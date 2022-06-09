package com.raphtory.api.input

/** Properties are characteristic attributes like name, etc. assigned to Vertices and Edges by the
  * [[Graph Builder]].
  * @see [[GraphBuilder]]
  */

/** Sealed trait defining different types of properties */
sealed trait Property {

  /** property name */
  def key: String

  /** value property value */
  def value: Any
}

/** Vertex/Edge type (this is not a `Property`) */
case class Type(name: String)

/** `Property` with a fixed value (the value should be the same for each update to the entity) */
case class ImmutableProperty(key: String, value: String) extends Property

/** `Property` with a `String` value */
case class StringProperty(key: String, value: String) extends Property

/** `Property` with a `Long` value */
case class LongProperty(key: String, value: Long) extends Property

/** `Property` with a `Double` value */
case class DoubleProperty(key: String, value: Double) extends Property

/** `Property` with a `Float` value */
case class FloatProperty(key: String, value: Float) extends Property

/** Wrapper class for properties */
case class Properties(property: Property*)

private[raphtory]sealed trait GraphAlteration

private[raphtory]object GraphAlteration

private[raphtory] sealed trait GraphUpdate extends GraphAlteration {
  val updateTime: Long
  val srcId: Long
}

/** basic update types */
private[raphtory] case class VertexAdd(
    updateTime: Long,
    srcId: Long,
    properties: Properties,
    vType: Option[Type]
)                                                                        extends GraphUpdate //add a vertex (or add/update a property to an existing vertex)
private[raphtory] case class VertexDelete(updateTime: Long, srcId: Long) extends GraphUpdate

private[raphtory] case class EdgeAdd(
    updateTime: Long,
    srcId: Long,
    dstId: Long,
    properties: Properties,
    eType: Option[Type]
) extends GraphUpdate

private[raphtory] case class EdgeDelete(updateTime: Long, srcId: Long, dstId: Long)
        extends GraphUpdate

/** Required sync after an update has been applied to a partition */
sealed abstract private[raphtory] class GraphUpdateEffect(val updateId: Long)
        extends GraphAlteration {
  val msgTime: Long
}

private[raphtory] case class SyncNewEdgeAdd(
    msgTime: Long,
    srcId: Long,
    dstId: Long,
    properties: Properties,
    removals: List[Long],
    vType: Option[Type]
) extends GraphUpdateEffect(dstId)

private[raphtory] case class BatchAddRemoteEdge(
    msgTime: Long,
    srcId: Long,
    dstId: Long,
    properties: Properties,
    vType: Option[Type]
) extends GraphUpdateEffect(dstId)

private[raphtory] case class SyncExistingEdgeAdd(
    msgTime: Long,
    srcId: Long,
    dstId: Long,
    properties: Properties
) extends GraphUpdateEffect(dstId)

private[raphtory] case class SyncExistingEdgeRemoval(msgTime: Long, srcId: Long, dstId: Long)
        extends GraphUpdateEffect(dstId)

private[raphtory] case class SyncNewEdgeRemoval(
    msgTime: Long,
    srcId: Long,
    dstId: Long,
    removals: List[Long]
) extends GraphUpdateEffect(dstId)

/** Edge removals generated via vertex removals */
private[raphtory] case class OutboundEdgeRemovalViaVertex(msgTime: Long, srcId: Long, dstId: Long)
        extends GraphUpdateEffect(dstId)

private[raphtory] case class InboundEdgeRemovalViaVertex(msgTime: Long, srcId: Long, dstId: Long)
        extends GraphUpdateEffect(srcId)

/** Responses from a partition receiving any of the above */
private[raphtory] case class SyncExistingRemovals(
    msgTime: Long,
    srcId: Long,
    dstId: Long,
    removals: List[Long],
    fromAddition: Boolean
) extends GraphUpdateEffect(srcId)

private[raphtory] case class EdgeSyncAck(
    msgTime: Long,
    srcId: Long,
    dstId: Long,
    fromAddition: Boolean
) extends GraphUpdateEffect(srcId)

private[raphtory] case class VertexRemoveSyncAck(msgTime: Long, override val updateId: Long)
        extends GraphUpdateEffect(updateId)
