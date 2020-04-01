package com.raphtory.core.analysis.Managers.ViewManagers

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.model.communication.AnalysisType

class WindowedViewAnalysisManager(managerCount:Int,jobID: String, analyser: Analyser, time: Long, window: Long)
        extends ViewAnalysisManager(managerCount,jobID: String, analyser, time: Long) {
  override def windowSize(): Long = window
  override def processResults(time: Long) =
    analyser.processWindowResults(result, timestamp(), windowSize(), viewCompleteTime)
  override protected def analysisType(): AnalysisType.Value = AnalysisType.view

}
