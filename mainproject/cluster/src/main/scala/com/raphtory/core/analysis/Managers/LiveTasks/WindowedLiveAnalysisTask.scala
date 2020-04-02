package com.raphtory.core.analysis.Managers.LiveTasks

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Managers.AnalysisTask
import com.raphtory.core.model.communication.AnalysisType

class WindowedLiveAnalysisTask(managerCount:Int, jobID: String, analyser: Analyser, windowSize:Long) extends AnalysisTask(jobID, analyser,managerCount) {
  override protected def analysisType(): AnalysisType.Value = AnalysisType.live
}
