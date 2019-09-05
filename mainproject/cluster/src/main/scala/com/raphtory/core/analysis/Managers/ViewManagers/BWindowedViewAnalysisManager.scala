package com.raphtory.core.analysis.Managers.ViewManagers

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.model.communication.AnalysisType

class BWindowedViewAnalysisManager(jobID:String, analyser:Analyser, time:Long, windows:Array[Long]) extends ViewAnalysisManager(jobID,analyser,time) {
  override def windowSet(): Array[Long] = windows
  override def processResults(): Unit = analyser.processBatchWindowResults(results,oldResults,timestamp(),windowSet())
  override protected def analysisType(): AnalysisType.Value = AnalysisType.view
}
