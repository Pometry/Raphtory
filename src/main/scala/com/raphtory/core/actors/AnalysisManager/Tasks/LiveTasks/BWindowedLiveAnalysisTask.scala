package com.raphtory.analysis.Tasks.LiveTasks

import com.raphtory.api.Analyser
import com.raphtory.core.model.communication.AnalysisType

import scala.collection.mutable.ArrayBuffer

class BWindowedLiveAnalysisTask(managerCount:Int, jobID: String,args:Array[String], analyser: Analyser,repeatTime:Long,eventTime:Boolean, windowset:Array[Long],newAnalyser:Boolean,rawFile:String)
  extends LiveAnalysisTask(managerCount,jobID, args,analyser,repeatTime,eventTime,newAnalyser,rawFile) {
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
  override def windowSet(): Array[Long]  = windowset.sortBy(x=>x)(sortOrdering)
  override protected def analysisType(): AnalysisType.Value = AnalysisType.live
  override def processResults(time: Long): Unit = {
    var i = 0
    val vtime = viewCompleteTime
    result().asInstanceOf[ArrayBuffer[ArrayBuffer[Any]]].foreach(res =>{
      analyser.processWindowResults(res, timestamp(), windowSet()(i), vtime)
      i+=1
    })
  }
}
