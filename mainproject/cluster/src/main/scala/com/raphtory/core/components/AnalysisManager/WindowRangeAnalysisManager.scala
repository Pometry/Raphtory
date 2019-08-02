package com.raphtory.core.components.AnalysisManager

import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.model.communication.AnalyserPresentCheck
import com.raphtory.core.utils.Utils

abstract class WindowRangeAnalysisManager(jobID:String,start:Long,end:Long,jump:Long,window:Long) extends RangeAnalysisManager(jobID,start,end,jump) {
  override def windowSize(): Long = window
}
