package com.raphtory.core.analysis.Managers.ViewTasks

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.model.communication.AnalysisType

class WindowedViewAnalysisTask(managerCount:Int, jobID: String, analyser: Analyser, time: Long, window: Long)
        extends ViewAnalysisTask(managerCount,jobID: String, analyser, time: Long) {
  override def windowSize(): Long = window
  override def processResults(time: Long) =
    analyser.processWindowResults(result, timestamp(), windowSize(), viewCompleteTime)
  override protected def analysisType(): AnalysisType.Value = AnalysisType.view

}
