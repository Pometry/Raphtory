package com.raphtory.core.analysis.AnalysisManager

import java.util.Date

import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.analysis.Analyser
import com.raphtory.core.model.communication.AnalyserPresentCheck
import com.raphtory.core.utils.Utils

class ViewAnalysisManager(jobID:String,analyser:Analyser,time:Long) extends LiveAnalysisManager(jobID:String,analyser) {
  override def timestamp():Long = time

  override def restart() = {
    println(s"View Analaysis manager for $jobID at ${new Date(time)} finished")
    System.exit(0)
  }

  override def processResults(): Unit = analyser.processViewResults(results,oldResults,timestamp())
}
//1471459626000L

