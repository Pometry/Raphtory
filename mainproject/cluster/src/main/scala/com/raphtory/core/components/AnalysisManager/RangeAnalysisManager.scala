package com.raphtory.core.components.AnalysisManager

import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.model.communication.AnalyserPresentCheck
import com.raphtory.core.utils.Utils

abstract class RangeAnalysisManager(jobID:String,start:Long,end:Long,jump:Long) extends LiveAnalysisManager(jobID:String) {
  private var currentTimestamp = start
  override def timestamp(): Long = currentTimestamp
  override def restart(): Unit = {
    if(currentTimestamp==end)
      return

    currentTimestamp = currentTimestamp +jump

    if(currentTimestamp>end)
      currentTimestamp==end

    for(worker <- Utils.getAllReaders(managerCount))
      mediator ! DistributedPubSubMediator.Send(worker, AnalyserPresentCheck(this.generateAnalyzer.getClass.getName.replace("$","")),false)

  }
}
