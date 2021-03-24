package com.raphtory.analysis.Tasks.RangeTasks

import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.analysis.api.Analyser
import com.raphtory.analysis.Tasks.AnalysisTask
import com.raphtory.analysis.Tasks.AnalysisTask.Message.AnalyserPresentCheck
import com.raphtory.core.model.communication.AnalysisType

class RangeAnalysisTask(managerCount:Int, jobID: String, args:Array[String],analyser: Analyser[Any], start: Long, end: Long, jump: Long,newAnalyser:Boolean,rawFile:String)
        extends AnalysisTask(jobID: String,args, analyser,managerCount,newAnalyser,rawFile) {
  protected var currentTimestamp                            = start
  override def restartTime() = 0
  override protected def analysisType(): AnalysisType.Value = AnalysisType.range
  override def timestamp(): Long                            = currentTimestamp
  override def restart(): Unit = {
    if (currentTimestamp == end) {
      println(s"Range Analysis manager for $jobID between ${start} and ${end} finished")
      //killme()
    }
    else {
      currentTimestamp = currentTimestamp + jump

      if (currentTimestamp > end)
        currentTimestamp = end

      for (worker <- getAllReaders(managerCount))
        mediator ! DistributedPubSubMediator
          .Send(worker, AnalyserPresentCheck(this.generateAnalyzer.getClass.getName.replace("$", "")), false)
    }


  }

  override def processResults(timestamp: Long): Unit =
    analyser.processResults(result, this.timestamp(), viewCompleteTime)
}
