package com.raphtory.core.actors.AnalysisManager.Tasks.Subtasks

import com.raphtory.core.actors.AnalysisManager.Tasks.{AnalysisTask, SubTaskController}
import com.raphtory.core.analysis.api.{AggregateSerialiser, Analyser}

final case class ViewAnalysisTask(
    managerCount: Int,
    jobId: String,
    args: Array[String],
    analyser: Analyser[Any],
    serialiser:AggregateSerialiser,
    time: Long,
    windows: List[Long],
    newAnalyser: Boolean,
    rawFile: String
) extends AnalysisTask(jobId: String, args, analyser, serialiser, managerCount, newAnalyser, rawFile) {
  override protected def buildSubTaskController(readyTimestamp: Long): SubTaskController =
    SubTaskController.fromViewTask(time, windows)
}
