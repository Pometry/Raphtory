package com.raphtory.core.components.AnalysisManager

import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.analysis.Analyser
import com.raphtory.core.model.communication.AnalyserPresentCheck
import com.raphtory.core.utils.Utils

class WindowRangeAnalysisManager(jobID:String,analyser:Analyser,start:Long,end:Long,jump:Long,window:Long) extends RangeAnalysisManager(jobID,analyser,start,end,jump) {
  override def windowSize(): Long = window
  override def processResults() = analyser.processWindowResults(results,oldResults,timestamp(),windowSize())
}
