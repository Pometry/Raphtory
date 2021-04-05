package com.raphtory.core.actors.AnalysisManager.Tasks.RangeTasks

import com.raphtory.core.actors.AnalysisManager.Tasks.AnalysisTask
import com.raphtory.core.actors.AnalysisManager.Tasks.SubTaskController
import com.raphtory.core.analysis.api.Analyser

final case class RangeAnalysisTask(
    managerCount: Int,
    jobID: String,
    args: Array[String],
    analyser: Analyser[Any],
    start: Long,
    end: Long,
    jump: Long,
    windows: List[Long],
    newAnalyser: Boolean,
    rawFile: String
) extends AnalysisTask(jobID: String, args, analyser, managerCount, newAnalyser, rawFile) {
  override protected def buildSubTaskController(readyTimestamp: Long): SubTaskController =
    SubTaskController.fromRangeTask(start, end, jump, windows)
}
