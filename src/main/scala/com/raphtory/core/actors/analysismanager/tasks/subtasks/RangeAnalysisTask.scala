package com.raphtory.core.actors.analysismanager.tasks.subtasks

import com.raphtory.core.actors.analysismanager.tasks.{AnalysisTask, SubTaskController}
import com.raphtory.core.analysis.api.{AggregateSerialiser, Analyser}

final case class RangeAnalysisTask(
    jobID: String,
    args: Array[String],
    analyser: Analyser[Any],
    serialiser:AggregateSerialiser,
    start: Long,
    end: Long,
    jump: Long,
    windows: List[Long],
    newAnalyser: Boolean,
    rawFile: String
) extends AnalysisTask(jobID: String, args, analyser, serialiser, newAnalyser, rawFile) {
  override protected def buildSubTaskController(readyTimestamp: Long): SubTaskController =
    SubTaskController.fromRangeTask(start, end, jump, windows)
}
