package com.raphtory.core.analysis.Managers.ViewManagers

import java.util.Date

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Managers.AnalysisManager
import com.raphtory.core.model.communication.AnalysisType

class ViewAnalysisManager(jobID: String, analyser: Analyser, time: Long) extends AnalysisManager(jobID: String, analyser) {
  override def timestamp(): Long = time

  override protected def analysisType(): AnalysisType.Value = AnalysisType.view


  override def restart() = {
    println(s"View Analysis manager for $jobID at ${new Date(time)} finished")
    System.exit(0)
  }

  override def processResults(timestamp: Long): Unit = analyser.processViewResults(result, this.timestamp(), viewCompleteTime)
}
