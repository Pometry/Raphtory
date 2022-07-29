package com.raphtory.internals.storage.pojograph.entities.internal

import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.internals.storage.pojograph.OrderedBuffer._
import scala.collection.mutable
import com.raphtory.internals.storage.pojograph.History
import com.raphtory.internals.storage.pojograph.entities.PojoEntityView

/** Represents Graph Entities (Edges and Vertices)
  *
  * Contains a Map of properties (currently String to string)
  * longs representing unique vertex ID's stored in subclasses
  *
  * @param creationTime ID of the message that created the entity
  * @param isInitialValue  Is the first moment this entity is referenced
  */
abstract private[raphtory] class PojoEntity(
    val creationTime: Long,
    val index: Long,
    isInitialValue: Boolean
) extends PojoEntityView {

  // Properties from that entity
  private var entityType: String                = ""
  val properties: mutable.Map[String, Property] = mutable.Map[String, Property]()

  // History of that entity
  val historyView: History[HistoricEvent]         = History()
  historyView.insert(HistoricEvent(creationTime, index, isInitialValue))
  var deletions: mutable.ListBuffer[(Long, Long)] = mutable.ListBuffer.empty

  // History of that entity
  def deletionList: List[(Long, Long)] = deletions.toList

  def setType(newType: Option[String]): Unit =
    newType match {
      case Some(t) => entityType = t
      case None    =>
    }

  def Type: String = entityType

  def revive(msgTime: Long, index: Long): Unit =
    historyView insert HistoricEvent(msgTime, index, event = true)

  def kill(msgTime: Long, index: Long): Unit = {
    historyView insert HistoricEvent(msgTime, index, event = false)
    deletions sortedAppend (msgTime, index)
  }

  def get(property: String): Option[Property] = properties.get(property)

  // Add or update the property from an edge or a vertex based, using the operator vertex + (k,v) to add new properties
  def +(msgTime: Long, index: Long, immutable: Boolean, key: String, value: Any): Unit =
    properties.get(key) match {
      case Some(p) =>
        p update (msgTime, index, value)
      case None    =>
        if (immutable) properties.put(key, new ImmutableProperty(msgTime, index, value))
        else
          properties.put(key, new MutableProperty(msgTime, index, value))
    }

  def wipe(): Unit = historyView.clear()
}
