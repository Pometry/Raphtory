package com.raphtory.core.actors.AnalysisManager.Tasks.Subtasks

import com.raphtory.core.actors.AnalysisManager.Tasks.{AnalysisTask, SubTaskController}
import com.raphtory.core.analysis.api.Analyser

final case class ViewAnalysisTask(
    managerCount: Int,
    jobId: String,
    args: Array[String],
    analyser: Analyser[Any],
    time: Long,
    windows: List[Long],
    newAnalyser: Boolean,
    rawFile: String
) extends AnalysisTask(jobId: String, args, analyser, managerCount, newAnalyser, rawFile) {
  override protected def buildSubTaskController(readyTimestamp: Long): SubTaskController =
    SubTaskController.fromViewTask(time, windows)
}
