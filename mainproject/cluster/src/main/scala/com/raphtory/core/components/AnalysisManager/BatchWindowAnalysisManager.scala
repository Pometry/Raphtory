package com.raphtory.core.components.AnalysisManager

abstract class BatchWindowAnalysisManager(jobID:String,start:Long,end:Long,jump:Long,windows:Array[Long]) extends RangeAnalysisManager(jobID,start,end,jump) {
  override def windowSet(): Array[Long] = windows
}