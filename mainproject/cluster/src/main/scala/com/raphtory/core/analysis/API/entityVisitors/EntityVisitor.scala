package com.raphtory.core.analysis.API.entityVisitors

import com.raphtory.core.components.PartitionManager.Workers.ViewJob
import com.raphtory.core.model.graphentities.Entity

import scala.collection.mutable

abstract class EntityVisitor(entity:Entity,viewJob:ViewJob) {

  def firstActivityAfter(time:Long) = viewboundHistory.filter(k => k._1 >= time).minBy(x=>x._1)._1
  def latestActivity() = viewboundHistory.head
  def earliestActivity() = viewboundHistory.minBy(k=> k._1)



  def viewboundHistory(): mutable.TreeMap[Long, Boolean] = {
    if(viewJob.window > 0)
      entity.previousState.filter(k => k._1 <= viewJob.timestamp && k._1 >= viewJob.timestamp-viewJob.window)
    else {
      entity.previousState.filter(k => k._1 <= viewJob.timestamp)
    }

  }
}
