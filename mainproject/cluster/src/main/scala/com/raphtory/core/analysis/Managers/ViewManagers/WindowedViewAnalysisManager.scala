package com.raphtory.core.analysis.Managers.ViewManagers

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Managers.AnalysisManager

class WindowedViewAnalysisManager(jobID:String,analyser:Analyser,time:Long,window:Long) extends ViewAnalysisManager(jobID:String,analyser,time:Long) {
  override def windowSize(): Long = window
  override def processResults() = analyser.processWindowResults(results,oldResults,timestamp(),windowSize())
}
