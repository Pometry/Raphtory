package com.raphtory.analysis.Tasks.RangeTasks

import com.raphtory.core.analysis.api.Analyser
import com.raphtory.core.model.communication.AnalysisType

class WindowedRangeAnalysisTask(managerCount:Int, jobID: String,args:Array[String], analyser: Analyser[Any], start: Long, end: Long, jump: Long, window: Long,newAnalyser:Boolean,rawFile:String)
        extends RangeAnalysisTask(managerCount,jobID, args, analyser, start, end, jump,newAnalyser,rawFile) {
  override def windowSize(): Long = window
  override def processResults(time: Long) =
    analyser.extractResults(result)
  override protected def analysisType(): AnalysisType.Value = AnalysisType.range
}
