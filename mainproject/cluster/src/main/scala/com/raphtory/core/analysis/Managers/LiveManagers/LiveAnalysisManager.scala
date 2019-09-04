package com.raphtory.core.analysis.Managers.LiveManagers

import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Managers.AnalysisManager
import com.raphtory.core.model.communication.AnalysisType

class LiveAnalysisManager(jobID:String,analyser:Analyser) extends AnalysisManager(jobID,analyser){
  override protected def analysisType(): AnalysisType.Value = AnalysisType.live
}
