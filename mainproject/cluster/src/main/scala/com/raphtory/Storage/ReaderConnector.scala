package com.raphtory.Storage
import com.raphtory.GraphEntities.{Entity, Property}
import com.raphtory.utils.KeyEnum

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
trait ReaderConnector {

  def lookupEntity(entityType : KeyEnum.Value, entityId : Long) : Boolean
  def getEntity(entityType : KeyEnum.Value, entityId : Long) : Option[_ <: Entity]

  def getHistory(entityType : KeyEnum.Value, entityId : Long) : mutable.TreeMap[Long, Boolean]
  def getProperties(entityType : KeyEnum.Value, entityId : Long) : TrieMap[String, Property]
  def getEntities(entityType : KeyEnum.Value) : Set[Long]
  def getEntitiesObjs(entityType : KeyEnum.Value) : TrieMap[Long, Entity]
  def getAssociatedEdges(entityId : Long) : mutable.Set[Long]

  def rangeQuery(entityType : String, entityId : Long, property : String, startTime : Long, endTime : Long) : Any
}
