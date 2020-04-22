package com.raphtory.core.analysis.Tasks.RangeTasks

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.model.communication.AnalysisType

import scala.collection.mutable.ArrayBuffer

class BWindowedRangeAnalysisTask(
    managerCount:Int,
    jobID: String,
    args:Array[String],
    analyser: Analyser,
    start: Long,
    end: Long,
    jump: Long,
    windows: Array[Long],
    rawFile:String
) extends RangeAnalysisTask(managerCount,jobID, args,analyser, start, end, jump,rawFile) {
  override def result(): ArrayBuffer[Any] = {
    val original = super.result()
    if (original.nonEmpty) {
      val invertedArray = ArrayBuffer[ArrayBuffer[Any]]()
      for (i <- original(0).asInstanceOf[ArrayBuffer[Any]].indices)
        invertedArray += new ArrayBuffer[Any]()
      original.foreach { x =>
        val internal = x.asInstanceOf[ArrayBuffer[Any]]
        for (j <- internal.indices)
          invertedArray(j) += internal(j)
      }
      invertedArray.asInstanceOf[ArrayBuffer[Any]]

    } else original
  }
  override def windowSet(): Array[Long]                     = windows
  override protected def analysisType(): AnalysisType.Value = AnalysisType.range
  override def processResults(time: Long): Unit =
    analyser.processBatchWindowResults(result, timestamp(), windowSet(), viewCompleteTime)
}
