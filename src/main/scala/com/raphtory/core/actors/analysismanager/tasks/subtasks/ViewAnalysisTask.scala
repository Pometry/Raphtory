package com.raphtory.core.actors.analysismanager.tasks.subtasks

import com.raphtory.core.actors.analysismanager.tasks.{AnalysisTask, SubTaskController}
import com.raphtory.core.analysis.api.{AggregateSerialiser, Analyser}

final case class ViewAnalysisTask(jobId: String, args: Array[String], analyser: Analyser[Any], serialiser:AggregateSerialiser, time: Long, windows: List[Long])
  extends AnalysisTask(jobId: String, args, analyser, serialiser) {
  override protected def buildSubTaskController(readyTimestamp: Long): SubTaskController =
    SubTaskController.fromViewTask(time, windows)
}
