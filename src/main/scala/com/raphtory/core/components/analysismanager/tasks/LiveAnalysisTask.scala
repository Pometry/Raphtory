package com.raphtory.core.components.analysismanager.tasks

import com.raphtory.core.model.algorithm.{AggregateSerialiser, Analyser}

final case class LiveAnalysisTask(jobID: String, args: Array[String], analyser: Analyser[Any], serialiser: AggregateSerialiser, repeatTime: Long, eventTime: Boolean, windows: List[Long])
  extends AnalysisTask(jobID, args, analyser, serialiser) {
  override protected def buildSubTaskController(readyTimestamp: Long): SubTaskController =
    if (eventTime) SubTaskController.fromEventLiveTask(repeatTime, windows, readyTimestamp)
    else SubTaskController.fromPureLiveTask(repeatTime, windows)
}
