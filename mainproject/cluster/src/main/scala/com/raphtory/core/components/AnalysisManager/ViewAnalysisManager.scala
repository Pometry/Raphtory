package com.raphtory.core.components.AnalysisManager

abstract class ViewAnalysisManager(init:(String,Long)) extends LiveAnalysisManager(init._1) {
  override def timestamp():Long = init._2
}
//1471459626000L