package com.raphtory.core.storage
import com.raphtory.core.model.graphentities.{Edge, Entity, Property}
import com.raphtory.core.utils.KeyEnum

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap
trait ReaderConnector {

  def lookupEntity(entityType : KeyEnum.Value, entityId : Long) : Boolean
  def getEntity(entityType : KeyEnum.Value, entityId : Long) : Option[_ <: Entity]

  def getHistory(entityType : KeyEnum.Value, entityId : Long) : mutable.TreeMap[Long, Boolean]
  def getProperties(entityType : KeyEnum.Value, entityId : Long) : ParTrieMap[String, Property]
  def getEntities(entityType : KeyEnum.Value) : Set[Long]
  def getEntitiesObjs(entityType : KeyEnum.Value) : ParTrieMap[_ <: AnyVal, _ <: Entity]
  def getAssociatedEdges(entityId : Long) : mutable.LinkedHashSet[Edge]

  def rangeQuery(entityType : String, entityId : Long, property : String, startTime : Long, endTime : Long) : Any
}
