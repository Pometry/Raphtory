package com.raphtory.core.analysis.Managers.RangeManagers

import java.util.Date

import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Managers.AnalysisManager
import com.raphtory.core.model.communication.{AnalyserPresentCheck, AnalysisType}
import com.raphtory.core.utils.Utils

class RangeAnalysisManager(jobID:String,analyser:Analyser,start:Long,end:Long,jump:Long) extends AnalysisManager(jobID:String,analyser) {
  protected var currentTimestamp = start
  override protected def analysisType(): AnalysisType.Value = AnalysisType.range
  override def timestamp(): Long = currentTimestamp
  override def restart(): Unit = {
    if(currentTimestamp==end){
      println(s"Range Analaysis manager for $jobID between ${new Date(start)} and ${new Date(end)} finished")
      System.exit(0)
    }
    currentTimestamp = currentTimestamp +jump

    if(currentTimestamp>end)
      currentTimestamp=end

    for(worker <- Utils.getAllReaders(managerCount))
      mediator ! DistributedPubSubMediator.Send(worker, AnalyserPresentCheck(this.generateAnalyzer.getClass.getName.replace("$","")),false)

  }

  override def processResults(): Unit = analyser.processViewResults(result,oldResult,timestamp())
}
