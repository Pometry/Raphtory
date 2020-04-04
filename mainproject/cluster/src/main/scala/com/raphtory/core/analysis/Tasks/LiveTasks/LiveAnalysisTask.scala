package com.raphtory.core.analysis.Tasks.LiveTasks

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Tasks.AnalysisTask
import com.raphtory.core.model.communication.AnalysisType

class LiveAnalysisTask(managerCount:Int, jobID: String, args:Array[String],analyser: Analyser) extends AnalysisTask(jobID,args, analyser,managerCount) {
  override protected def analysisType(): AnalysisType.Value = AnalysisType.live
}
