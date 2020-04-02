package com.raphtory.core.analysis.Tasks.RangeTasks

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.model.communication.AnalysisType

class WindowedRangeAnalysisTask(managerCount:Int, jobID: String, analyser: Analyser, start: Long, end: Long, jump: Long, window: Long)
        extends RangeAnalysisTask(managerCount,jobID, analyser, start, end, jump) {
  override def windowSize(): Long = window
  override def processResults(time: Long) =
    analyser.processWindowResults(result, timestamp(), windowSize(), viewCompleteTime)
  override protected def analysisType(): AnalysisType.Value = AnalysisType.range
}
