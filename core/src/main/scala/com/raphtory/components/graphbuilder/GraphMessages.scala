package com.raphtory.components.graphbuilder

import org.apache.pulsar.client.api.Schema
import Properties._

/**
  * # Properties
  *
  * Properties are characteristic attributes like name, etc. assigned to Vertices and Edges by the [Graph Builder](com.raphtory.components.graphbuilder.GraphBuilder).
  *
  * ## Members
  *
  * {s}`Type(name: String)`
  *   : Vertex/Edge type (this is not a {s}`Property`)
  *
  * {s}`Property`
  *  : sealed trait defining different types of properties
  *
  *    **Attributes**
  *
  *    {s}`key: String`
  *      : property name
  *
  *    {s}`value: Any`
  *      : property value
  *
  * {s}`Properties(property: Property*)`
  *   : Wrapper class for properties
  *
  * {s}`ImmutableProperty(key: String, value: String)`
  *   : {s}`Property` with a fixed value (the value should be the same for each update to the entity)
  *
  * {s}`StringProperty(key: String, value: String)`
  *   : {s}`Property` with a {s}`String` value
  *
  * {s}`LongProperty(key: String, value: Long)`
  *   : {s}`Property` with a {s}`Long` value
  *
  * {s}`DoubleProperty(key: String, value: Double)`
  *   : {s}`Property` with a {s}`Double` value
  *
  * {s}`FloatProperty(key: String, value: Float)`
  *   : {s}`Property` with a {s}`Float` value
  *
  * ```{seealso}
  * [](com.raphtory.components.graphbuilder.GraphBuilder)
  * ```
  */
object Properties {

  sealed trait Property {
    def key: String
    def value: Any
  }
  case class Type(name: String)

  case class ImmutableProperty(key: String, value: String) extends Property

  case class StringProperty(key: String, value: String) extends Property

  case class LongProperty(key: String, value: Long) extends Property

  case class DoubleProperty(key: String, value: Double) extends Property

  case class FloatProperty(key: String, value: Float) extends Property

  case class Properties(property: Property*)
}

sealed trait GraphAlteration

object GraphAlteration

sealed trait GraphUpdate extends GraphAlteration {
  val updateTime: Long
  val srcId: Long
}

//basic update types
case class VertexAdd(updateTime: Long, srcId: Long, properties: Properties, vType: Option[Type])
        extends GraphUpdate //add a vertex (or add/update a property to an existing vertex)
case class VertexDelete(updateTime: Long, srcId: Long) extends GraphUpdate

case class EdgeAdd(
    updateTime: Long,
    srcId: Long,
    dstId: Long,
    properties: Properties,
    eType: Option[Type]
)                                                                 extends GraphUpdate
case class EdgeDelete(updateTime: Long, srcId: Long, dstId: Long) extends GraphUpdate

//Required sync after an update has been applied to a partition
sealed abstract class GraphUpdateEffect(val updateId: Long) extends GraphAlteration {
  val msgTime: Long
}

case class SyncNewEdgeAdd(
    msgTime: Long,
    srcId: Long,
    dstId: Long,
    properties: Properties,
    removals: List[Long],
    vType: Option[Type]
) extends GraphUpdateEffect(dstId)

case class BatchAddRemoteEdge(
    msgTime: Long,
    srcId: Long,
    dstId: Long,
    properties: Properties,
    vType: Option[Type]
) extends GraphUpdateEffect(dstId)

case class SyncExistingEdgeAdd(msgTime: Long, srcId: Long, dstId: Long, properties: Properties)
        extends GraphUpdateEffect(dstId)

case class SyncExistingEdgeRemoval(msgTime: Long, srcId: Long, dstId: Long)
        extends GraphUpdateEffect(dstId)

case class SyncNewEdgeRemoval(msgTime: Long, srcId: Long, dstId: Long, removals: List[Long])
        extends GraphUpdateEffect(dstId)

//Edge removals generated via vertex removals
case class OutboundEdgeRemovalViaVertex(msgTime: Long, srcId: Long, dstId: Long)
        extends GraphUpdateEffect(dstId)

case class InboundEdgeRemovalViaVertex(msgTime: Long, srcId: Long, dstId: Long)
        extends GraphUpdateEffect(srcId)

//Responses from a partition receiving any of the above
case class SyncExistingRemovals(
    msgTime: Long,
    srcId: Long,
    dstId: Long,
    removals: List[Long],
    fromAddition: Boolean
) extends GraphUpdateEffect(srcId)

case class EdgeSyncAck(msgTime: Long, srcId: Long, dstId: Long, fromAddition: Boolean)
        extends GraphUpdateEffect(srcId)

case class VertexRemoveSyncAck(msgTime: Long, override val updateId: Long)
        extends GraphUpdateEffect(updateId)
