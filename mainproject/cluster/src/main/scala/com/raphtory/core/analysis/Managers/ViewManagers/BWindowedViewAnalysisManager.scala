package com.raphtory.core.analysis.Managers.ViewManagers

import com.raphtory.core.analysis.API.Analyser

class BWindowedViewAnalysisManager(jobID:String, analyser:Analyser, time:Long, windows:Array[Long]) extends ViewAnalysisManager(jobID,analyser,time) {
  override def windowSet(): Array[Long] = windows

}
