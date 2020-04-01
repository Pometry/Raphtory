package com.raphtory.core.clustersetup

/**
  * Created by Mirate on 29/09/2017.
  */
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Managers.AnalysisManager
import com.raphtory.core.analysis.Managers.LiveManagers.BWindowedLiveAnalysisManager
import com.raphtory.core.analysis.Managers.LiveManagers.LiveAnalysisManager
import com.raphtory.core.analysis.Managers.LiveManagers.WindowedLiveAnalysisManager
import com.raphtory.core.analysis.Managers.RangeManagers.BWindowedRangeAnalysisManager
import com.raphtory.core.analysis.Managers.RangeManagers.RangeAnalysisManager
import com.raphtory.core.analysis.Managers.RangeManagers.WindowedRangeAnalysisManager
import com.raphtory.core.analysis.Managers.ViewManagers.BWindowedViewAnalysisManager
import com.raphtory.core.analysis.Managers.ViewManagers.ViewAnalysisManager
import com.raphtory.core.analysis.Managers.ViewManagers.WindowedViewAnalysisManager

import akka.actor.Actor
import akka.actor.Props
import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Managers.RangeManagers.BWindowedRangeAnalysisManager
import com.raphtory.core.analysis.Managers.RangeManagers.RangeAnalysisManager
import com.raphtory.core.analysis.Managers.RangeManagers.WindowedRangeAnalysisManager
import com.raphtory.core.analysis.Managers.ViewManagers.BWindowedViewAnalysisManager
import com.raphtory.core.analysis.Managers.ViewManagers.ViewAnalysisManager
import com.raphtory.core.analysis.Managers.ViewManagers.WindowedViewAnalysisManager
import com.raphtory.core.analysis.Managers.LiveManagers.BWindowedLiveAnalysisManager
import com.raphtory.core.analysis.Managers.LiveManagers.LiveAnalysisManager
import com.raphtory.core.analysis.Managers.LiveManagers.WindowedLiveAnalysisManager

case class LiveAnalysisNode(seedLoc: String, name: String) extends DocSvr {
  implicit val system = initialiseActorSystem(List(seedLoc))



}
