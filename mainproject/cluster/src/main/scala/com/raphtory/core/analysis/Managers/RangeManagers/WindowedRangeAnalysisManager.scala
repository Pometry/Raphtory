package com.raphtory.core.analysis.Managers.RangeManagers

import com.raphtory.core.analysis.API.Analyser

class WindowedRangeAnalysisManager(jobID:String, analyser:Analyser, start:Long, end:Long, jump:Long, window:Long) extends RangeAnalysisManager(jobID,analyser,start,end,jump) {
  override def windowSize(): Long = window
  override def processResults() = analyser.processWindowResults(results,oldResults,timestamp(),windowSize())
}
