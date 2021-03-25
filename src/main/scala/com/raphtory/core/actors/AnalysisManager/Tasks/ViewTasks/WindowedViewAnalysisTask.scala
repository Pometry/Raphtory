package com.raphtory.core.actors.AnalysisManager.Tasks.ViewTasks

import com.raphtory.core.analysis.api.Analyser
import com.raphtory.core.model.communication.AnalysisType

class WindowedViewAnalysisTask(managerCount:Int, jobID: String,args:Array[String], analyser: Analyser[Any], time: Long, window: Long,newAnalyser:Boolean,rawFile:String)
        extends ViewAnalysisTask(managerCount,jobID: String,args, analyser, time: Long,newAnalyser,rawFile) {
  override def windowSize(): Long = window
  override def processResults(time: Long) =
    analyser.extractResults(result)
  override protected def analysisType(): AnalysisType.Value = AnalysisType.view

}
