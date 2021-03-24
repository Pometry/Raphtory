package com.raphtory.analysis.Tasks.LiveTasks

import com.raphtory.core.analysis.api.Analyser
import com.raphtory.core.model.communication.AnalysisType

import scala.collection.mutable.ArrayBuffer

class BWindowedLiveAnalysisTask(managerCount:Int, jobID: String,args:Array[String], analyser: Analyser[Any],repeatTime:Long,eventTime:Boolean, windowset:Array[Long],newAnalyser:Boolean,rawFile:String)
  extends LiveAnalysisTask(managerCount,jobID, args,analyser,repeatTime,eventTime,newAnalyser,rawFile) {
  override def result(): Array[Any] = {
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
      invertedArray.asInstanceOf[ArrayBuffer[Any]].toArray
    } else original
  }
  override def windowSet(): Array[Long]  = windowset.sortBy(x=>x)(sortOrdering)
  override protected def analysisType(): AnalysisType.Value = AnalysisType.live
  override def processResults(time: Long): Unit = {
    var i = 0
    val vtime = viewCompleteTime
    result().asInstanceOf[Array[Array[Any]]].foreach(res =>{
      analyser.extractResults(res)
      i+=1
    })
  }
}
