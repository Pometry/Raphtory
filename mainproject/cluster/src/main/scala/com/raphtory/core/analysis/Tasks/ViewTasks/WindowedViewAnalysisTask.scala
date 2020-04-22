package com.raphtory.core.analysis.Tasks.ViewTasks

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.model.communication.AnalysisType

class WindowedViewAnalysisTask(managerCount:Int, jobID: String,args:Array[String], analyser: Analyser, time: Long, window: Long,rawFile:String)
        extends ViewAnalysisTask(managerCount,jobID: String,args, analyser, time: Long,rawFile) {
  override def windowSize(): Long = window
  override def processResults(time: Long) =
    analyser.processWindowResults(result, timestamp(), windowSize(), viewCompleteTime)
  override protected def analysisType(): AnalysisType.Value = AnalysisType.view

}
