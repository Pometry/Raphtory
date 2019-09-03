package com.raphtory.core.analysis.Managers.ViewManagers

import java.util.Date

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Managers.AnalysisManager

class ViewAnalysisManager(jobID:String,analyser:Analyser,time:Long) extends AnalysisManager(jobID:String,analyser) {
  override def timestamp():Long = time

  override def restart() = {
    println(s"View Analaysis manager for $jobID at ${new Date(time)} finished")
    System.exit(0)
  }

  override def processResults(): Unit = analyser.processViewResults(results,oldResults,timestamp())
}
//1471459626000L

