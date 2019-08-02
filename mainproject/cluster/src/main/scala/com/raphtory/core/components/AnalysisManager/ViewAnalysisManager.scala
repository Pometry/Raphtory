package com.raphtory.core.components.AnalysisManager

abstract class ViewAnalysisManager(jobID:String,time:Long) extends LiveAnalysisManager(jobID:String) {
  override def timestamp():Long = time
}
//1471459626000L