package com.raphtory.core.analysis.Tasks.LiveTasks

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Tasks.AnalysisTask
import com.raphtory.core.model.communication.AnalysisType

class WindowedLiveAnalysisTask(managerCount:Int, jobID: String,args:Array[String], analyser: Analyser,repeatTime:Long,eventTime:Boolean, windowSize:Long,rawFile:String)
  extends LiveAnalysisTask(managerCount,jobID, args,analyser,repeatTime,eventTime,rawFile:String) {
  override protected def analysisType(): AnalysisType.Value = AnalysisType.live
}
