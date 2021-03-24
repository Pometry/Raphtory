package com.raphtory.core.analysis.entity

import com.raphtory.core.actors.PartitionManager.Workers.ViewJob
import com.raphtory.core.model.entities.RaphtoryEntity

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

abstract class EntityVisitor(entity:RaphtoryEntity, viewJob:ViewJob) {
  def Type() = entity.getType

  def firstActivityAfter(time:Long) = getHistory.filter(k => k._1 >= time).minBy(x=>x._1)._1
  def latestActivity() = getHistory.head
  def earliestActivity() = getHistory.minBy(k=> k._1)

  def getPropertySet(): ParTrieMap[String,Any] = {
    val x =entity.properties.filter(p =>{
      p._2.creation()<=viewJob.timestamp
    }).map(f => (f._1,f._2.valueAt(viewJob.timestamp)))
//    if(x.isEmpty&&this.isInstanceOf[VertexVisitor]){
//      println(viewJob)
//    }
    x
  }

  def getPropertyValue(key: String): Option[Any] =
    entity.properties.get(key) match {
      case Some(p) => Some(p.valueAt(viewJob.timestamp))
      case None    => None
    }

  def getPropertyValueAt(key: String,time:Long): Option[Any] =
    entity.properties.get(key) match {
      case Some(p) => Some(p.valueAt(time))
      case None    => None
    }
  def getPropertyValuesAfter(key: String,time:Long): Option[Array[Any]] =
    entity.properties.get(key) match {
      case Some(p) => Some(p.valuesAfter(time))
      case None    => None
    }

  def getPropertyHistory(key:String): Option[Array[(Long,Any)]] = {
    entity.properties.get(key) match {
      case Some(p) => {
        if(viewJob.window > 0)
          Some(p.values().filter(k => k._1 <= viewJob.timestamp && k._1 >= viewJob.timestamp-viewJob.window))
        else {
          Some(p.values().filter(k => k._1 <= viewJob.timestamp))
        }
      }
      case None    => None
    }
  }

  def getHistory(): mutable.TreeMap[Long, Boolean] = {
    if(viewJob.window > 0)
      entity.history.filter(k => k._1 <= viewJob.timestamp && k._1 >= viewJob.timestamp-viewJob.window)
    else {
      entity.history.filter(k => k._1 <= viewJob.timestamp)
    }
  }

  //TODO only here for temp, needs to be removed
  protected def getReader(srcId: Long, managerCount: Int): String = {
    val mod     = srcId.abs % (managerCount * 10)
    val manager = mod / 10
    val worker  = mod % 10
    s"/user/Manager_${manager}_reader_$worker"
  }
}
