package com.raphtory.core.actors.AnalysisManager.Tasks.LiveTasks

import com.raphtory.core.analysis.api.Analyser
import com.raphtory.core.model.communication.AnalysisType

class WindowedLiveAnalysisTask(managerCount:Int, jobID: String,args:Array[String], analyser: Analyser[Any],repeatTime:Long,eventTime:Boolean, windowSize:Long,newAnalyser:Boolean,rawFile:String)
  extends LiveAnalysisTask(managerCount,jobID, args,analyser,repeatTime,eventTime,newAnalyser,rawFile:String) {
  override protected def analysisType(): AnalysisType.Value = AnalysisType.live
}
