package com.raphtory.core.analysis.Managers.LiveManagers

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Managers.AnalysisManager
import com.raphtory.core.model.communication.AnalysisType

class WindowedLiveAnalysisManager(managerCount:Int,jobID: String, analyser: Analyser,windowSize:Long) extends AnalysisManager(jobID, analyser,managerCount) {
  override protected def analysisType(): AnalysisType.Value = AnalysisType.live
}
