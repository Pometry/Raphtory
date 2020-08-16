package com.raphtory.core.analysis.API.entityVisitors

import com.raphtory.core.components.PartitionManager.Workers.ViewJob
import com.raphtory.core.model.graphentities.Entity

import scala.collection.mutable
import scala.collection.parallel.ParSet

abstract class EntityVisitor(entity:Entity,viewJob:ViewJob) {
  def Type() = entity.getType

  def firstActivityAfter(time:Long) = getHistory.filter(k => k._1 >= time).minBy(x=>x._1)._1
  def latestActivity() = getHistory.head
  def earliestActivity() = getHistory.minBy(k=> k._1)

  def getPropertySet(): ParSet[String] = entity.properties.filter(p =>{
    p._2.creation()<=viewJob.timestamp
  }).keySet

  def getPropertyValue(key: String): Option[Any] =
    entity.properties.get(key) match {
      case Some(p) => Some(p.valueAt(viewJob.timestamp))
      case None    => None
    }

  def getHistory(): mutable.TreeMap[Long, Boolean] = {
    if(viewJob.window > 0)
      entity.history.filter(k => k._1 <= viewJob.timestamp && k._1 >= viewJob.timestamp-viewJob.window)
    else {
      entity.history.filter(k => k._1 <= viewJob.timestamp)
    }

  }
}
