package com.raphtory.core.model.communication

import com.raphtory.core.model.implementations.entities.RaphtoryEdge

import scala.collection.mutable

/**
  * Created by Mirate on 30/05/2017.
  */
sealed trait Property {
  def key: String
  def value: Any
}

case class Type(name: String)
case class ImmutableProperty(key: String, value: String) extends Property
case class StringProperty(key: String, value: String)    extends Property
case class LongProperty(key: String, value: Long)        extends Property
case class DoubleProperty(key: String, value: Double)    extends Property
case class FloatProperty(key: String, value: Float)    extends Property
case class Properties(property: Property*)

sealed trait GraphUpdate {
  val updateTime: Long
  val srcId: Long
}

case class VertexAdd(updateTime: Long, srcId: Long, properties: Properties, vType: Option[Type]) extends GraphUpdate //add a vertex (or add/update a property to an existing vertex)
case class VertexDelete(updateTime: Long, srcId: Long) extends GraphUpdate
case class EdgeAdd(updateTime: Long, srcId: Long, dstId: Long, properties: Properties, eType: Option[Type]) extends GraphUpdate
case class EdgeDelete(updateTime: Long, srcId: Long, dstId: Long) extends GraphUpdate

case class TrackedGraphUpdate[+T <: GraphUpdate](channelId: String, channelTime:Int, update: T)

sealed abstract class GraphUpdateEffect(val updateId: Long) extends Serializable {
  val msgTime: Long
}

case class RemoteEdgeAdd(msgTime: Long, srcId: Long, dstId: Long, properties: Properties) extends GraphUpdateEffect(dstId)
case class RemoteEdgeRemoval(msgTime: Long, srcId: Long, dstId: Long) extends GraphUpdateEffect(dstId)
case class RemoteEdgeRemovalFromVertex(msgTime: Long, srcId: Long, dstId: Long) extends GraphUpdateEffect(dstId)
case class RemoteEdgeAddNew(msgTime: Long, srcId: Long, dstId: Long, properties: Properties, kills: List[(Long, Boolean)], vType: Option[Type]) extends GraphUpdateEffect(dstId)
case class RemoteEdgeRemovalNew(msgTime: Long, srcId: Long, dstId: Long, kills: List[(Long, Boolean)]) extends GraphUpdateEffect(dstId)
case class RemoteReturnDeaths(msgTime: Long, srcId: Long, dstId: Long, kills: List[(Long, Boolean)]) extends GraphUpdateEffect(srcId)
case class ReturnEdgeRemoval(msgTime: Long, srcId: Long, dstId: Long) extends GraphUpdateEffect(srcId)

case class EdgeSyncAck(msgTime: Long, srcId: Long) extends GraphUpdateEffect(srcId)
case class VertexRemoveSyncAck(msgTime: Long, override val updateId: Long) extends GraphUpdateEffect(updateId)

case class TrackedGraphEffect[T <: GraphUpdateEffect](channelId: String, channelTime: Int, effect: T)

case class VertexMessage(vertexId: Long, data: Any)
