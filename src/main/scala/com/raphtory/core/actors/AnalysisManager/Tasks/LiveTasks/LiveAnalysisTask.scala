package com.raphtory.core.actors.AnalysisManager.Tasks.LiveTasks

import com.raphtory.core.actors.AnalysisManager.Tasks.AnalysisTask
import com.raphtory.core.actors.AnalysisManager.Tasks.SubTaskController
import com.raphtory.core.analysis.api.Analyser

final case class LiveAnalysisTask(
    managerCount: Int,
    jobID: String,
    args: Array[String],
    analyser: Analyser[Any],
    repeatTime: Long,
    eventTime: Boolean,
    windows: List[Long],
    newAnalyser: Boolean,
    rawFile: String
) extends AnalysisTask(jobID, args, analyser, managerCount, newAnalyser, rawFile) {
  override protected def buildSubTaskController(readyTimestamp: Long): SubTaskController =
    if (eventTime) SubTaskController.fromEventLiveTask(repeatTime, windows, readyTimestamp)
    else SubTaskController.fromPureLiveTask(repeatTime, windows)
}
