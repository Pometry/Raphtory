package com.raphtory.core.components.AnalysisManager

import com.raphtory.core.analysis.Analyser

abstract class BatchWindowAnalysisManager(jobID:String,analyser:Analyser,start:Long,end:Long,jump:Long,windows:Array[Long]) extends RangeAnalysisManager(jobID,analyser,start,end,jump) {
  override def windowSet(): Array[Long] = windows
}