package com.raphtory.analysis.Tasks.LiveTasks

import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.api.Analyser
import com.raphtory.analysis.Tasks.AnalysisTask
import com.raphtory.core.model.communication.{AnalysisType, Finish, Setup, TimeCheck}
import com.raphtory.core.utils.Utils

import scala.collection.mutable
import scala.concurrent.duration.{Duration, MILLISECONDS}

class LiveAnalysisTask(managerCount:Int, jobID: String, args:Array[String],analyser: Analyser,repeatTime:Long,eventTime:Boolean,newAnalyser:Boolean,rawFile:String)
  extends AnalysisTask(jobID,args, analyser,managerCount,newAnalyser,rawFile) {
  //implicit val executionContext = context.system.dispatchers.lookup("misc-dispatcher")
  override protected def analysisType(): AnalysisType.Value = AnalysisType.live
  protected var currentTimestamp = 1L
  override def timestamp(): Long = currentTimestamp
  private var liveTimes:mutable.Set[Long]  = mutable.Set[Long]()
  private var liveTime = 0l
  private var firstTime = true
  def liveTimestamp():Long     =  liveTime
  def setLiveTime() = {
    liveTime = liveTimes.min
    liveTimes = mutable.Set[Long]()
    if(!eventTime||firstTime){
      firstTime=false
      currentTimestamp=liveTime
    }
  }
  def resetTimes() = {
      liveTimes = mutable.Set[Long]()
  }

  override def restartTime(): Long = { //if processing time we just waiting that long
    if(eventTime) 0 else repeatTime
  }

  override def restart() = {
    if (repeatTime>0) {//if we want to restart

      if(eventTime){ //if its event time then we wait for the repeat time to be represented in the storage
          currentTimestamp=liveTime+repeatTime
          for (worker <- Utils.getAllReaderWorkers(managerCount))
            mediator ! DistributedPubSubMediator.Send(worker, TimeCheck(timestamp()), false)
      }
      else{
          currentTimestamp=liveTime
          for (worker <- Utils.getAllReaderWorkers(managerCount))
            mediator ! DistributedPubSubMediator.Send(worker, TimeCheck(timestamp), false)
      }
    }
  }


  override def timeResponse(ok: Boolean, time: Long) = {
    if (!ok)
      TimeOKFlag = false
    TimeOKACKS += 1
    liveTimes += time
    if (TimeOKACKS == getWorkerCount) {
      stepCompleteTime() //reset step counter
      if (TimeOKFlag) {
        setLiveTime()
        if (analyser.defineMaxSteps() > 1)
          for (worker <- Utils.getAllReaderWorkers(managerCount))
            mediator ! DistributedPubSubMediator.Send(
              worker,
              Setup(
                this.generateAnalyzer,
                jobID,
                args,
                currentSuperStep,
                timestamp,
                analysisType(),
                windowSize(),
                windowSet()
              ),
              false
            )
        else
          for (worker <- Utils.getAllReaderWorkers(managerCount))
            mediator ! DistributedPubSubMediator.Send(
              worker,
              Finish(
                this.generateAnalyzer,
                jobID,
                args,
                currentSuperStep,
                timestamp,
                analysisType(),
                windowSize(),
                windowSet()
              ),
              false
            )
      } else {
        //println(s"${timestamp()} is yet to be ingested, currently at ${time}. Retrying analysis in 1 seconds and retrying")
        resetTimes()
        context.system.scheduler.scheduleOnce(Duration(1000, MILLISECONDS), self, "recheckTime")
      }

      TimeOKACKS = 0
      TimeOKFlag = true
    }
  }

}
