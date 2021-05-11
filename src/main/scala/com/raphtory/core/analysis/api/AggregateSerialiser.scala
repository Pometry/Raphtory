package com.raphtory.core.analysis.api


abstract class AggregateSerialiser {
  def serialiseView(results:Map[String,Any],timestamp: Long,jobID:String,viewTime:Long)
  def serialiseWindowedView(results:Map[String,Any],timestamp: Long,window:Long,jobID:String,viewTime:Long)
}
