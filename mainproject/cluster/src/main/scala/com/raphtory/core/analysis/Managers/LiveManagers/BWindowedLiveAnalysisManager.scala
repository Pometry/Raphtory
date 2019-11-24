package com.raphtory.core.analysis.Managers.LiveManagers

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Managers.AnalysisManager
import com.raphtory.core.model.communication.AnalysisType

import scala.collection.mutable.ArrayBuffer

class BWindowedLiveAnalysisManager(jobID:String, analyser:Analyser) extends AnalysisManager(jobID,analyser) {
  override def result(): ArrayBuffer[Any] = {
    val original = super.result()
    if(original.nonEmpty){
      val invertedArray = ArrayBuffer[ArrayBuffer[Any]]()
      for(i <- original(0).asInstanceOf[ArrayBuffer[Any]].indices)
        invertedArray += new ArrayBuffer[Any]()
      original.foreach(x=> {
        val internal = x.asInstanceOf[ArrayBuffer[Any]]
        for(j <- internal.indices){
          invertedArray(j) += internal(j)
        }
      })
      invertedArray.asInstanceOf[ArrayBuffer[Any]]
    }
    else original
  }
   override protected def analysisType(): AnalysisType.Value = AnalysisType.live
}
