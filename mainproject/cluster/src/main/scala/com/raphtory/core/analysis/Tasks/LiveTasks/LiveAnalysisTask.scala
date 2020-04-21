package com.raphtory.core.analysis.Tasks.LiveTasks

import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Tasks.AnalysisTask
import com.raphtory.core.model.communication.{AnalyserPresentCheck, AnalysisType, TimeCheck}
import com.raphtory.core.utils.Utils

class LiveAnalysisTask(managerCount:Int, jobID: String, args:Array[String],analyser: Analyser,repeatTime:Long,eventTime:Boolean) extends AnalysisTask(jobID,args, analyser,managerCount) {
  override protected def analysisType(): AnalysisType.Value = AnalysisType.live
  protected var currentTimestamp = 1L

  override def restartTime(): Long = { //if processing time we just waiting that long
    if(eventTime) 0 else repeatTime
  }
  override  def timeRecheck() = //have to override so that event time is handled correctly
    if(eventTime){ //if its event time then we wait for the repeat time to be represented in the storage
      for (worker <- Utils.getAllReaderWorkers(managerCount))
        mediator ! DistributedPubSubMediator.Send(worker, TimeCheck(currentTimestamp + repeatTime), false)
    }
    else{
      for (worker <- Utils.getAllReaderWorkers(managerCount))
        mediator ! DistributedPubSubMediator.Send(worker, TimeCheck(timestamp), false)
    }

  override def restart() = {
    if (repeatTime>0) {//if we want to restart
      if(eventTime){ //if its event time then we wait for the repeat time to be represented in the storage
        currentTimestamp = liveTimestamp()
          for (worker <- Utils.getAllReaderWorkers(managerCount))
            mediator ! DistributedPubSubMediator.Send(worker, TimeCheck(currentTimestamp + repeatTime), false)
      }
      else{
          for (worker <- Utils.getAllReaderWorkers(managerCount))
            mediator ! DistributedPubSubMediator.Send(worker, TimeCheck(timestamp), false)
      }
    }
  }
}
