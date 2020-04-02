package com.raphtory.core.analysis.Managers.ViewTasks

import java.util.Date

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Managers.AnalysisTask
import com.raphtory.core.model.communication.AnalysisType

class ViewAnalysisTask(managerCount:Int, jobID: String, analyser: Analyser, time: Long)
        extends AnalysisTask(jobID: String, analyser,managerCount) {
  override def timestamp(): Long = time

  override protected def analysisType(): AnalysisType.Value = AnalysisType.view

  override def restart(): Unit = {
    println(s"View Analysis manager for $jobID at ${time} finished")
    killme()
  }

  override def processResults(timestamp: Long): Unit =
    analyser.processViewResults(result, this.timestamp(), viewCompleteTime)
}
