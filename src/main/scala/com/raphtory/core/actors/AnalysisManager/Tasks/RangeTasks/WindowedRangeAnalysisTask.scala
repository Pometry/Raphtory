package com.raphtory.analysis.Tasks.RangeTasks

import com.raphtory.api.Analyser
import com.raphtory.core.model.communication.AnalysisType

class WindowedRangeAnalysisTask(managerCount:Int, jobID: String,args:Array[String], analyser: Analyser, start: Long, end: Long, jump: Long, window: Long,newAnalyser:Boolean,rawFile:String)
        extends RangeAnalysisTask(managerCount,jobID, args, analyser, start, end, jump,newAnalyser,rawFile) {
  override def windowSize(): Long = window
  override def processResults(time: Long) =
    analyser.processWindowResults(result, timestamp(), windowSize(), viewCompleteTime)
  override protected def analysisType(): AnalysisType.Value = AnalysisType.range
}
