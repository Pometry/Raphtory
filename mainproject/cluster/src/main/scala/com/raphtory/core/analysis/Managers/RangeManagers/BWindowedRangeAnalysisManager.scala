package com.raphtory.core.analysis.Managers.RangeManagers

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.model.communication.AnalysisType

class BWindowedRangeAnalysisManager(jobID:String, analyser:Analyser, start:Long, end:Long, jump:Long, windows:Array[Long]) extends RangeAnalysisManager(jobID,analyser,start,end,jump) {
  override def windowSet(): Array[Long] = windows
  override protected def analysisType(): AnalysisType.Value = AnalysisType.range
}