package com.raphtory.core.analysis.Managers.ViewManagers

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Managers.AnalysisManager
import com.raphtory.core.model.communication.AnalysisType

class WindowedViewAnalysisManager(jobID:String,analyser:Analyser,time:Long,window:Long) extends ViewAnalysisManager(jobID:String,analyser,time:Long) {
  override def windowSize(): Long = window
  override def processResults() = analyser.processWindowResults(result,oldResult,timestamp(),windowSize())
  override protected def analysisType(): AnalysisType.Value = AnalysisType.view

}
