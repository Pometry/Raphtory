package com.raphtory.core.analysis.Managers.ViewManagers

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.model.communication.AnalysisType

import scala.collection.mutable.ArrayBuffer

class BWindowedViewAnalysisManager(jobID: String, analyser: Analyser, time: Long, windows: Array[Long])
        extends ViewAnalysisManager(jobID, analyser, time) {
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
  override def windowSet(): Array[Long] = windows
  override def processResults(time: Long): Unit =
    analyser.processBatchWindowResults(result, timestamp(), windowSet(), viewCompleteTime)
  override protected def analysisType(): AnalysisType.Value = AnalysisType.view
}
