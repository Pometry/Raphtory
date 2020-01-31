package com.raphtory.core.clustersetup

/**
  * Created by Mirate on 29/09/2017.
  */

import akka.actor.{ActorRef, ActorSystem, Props}
import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Managers.AnalysisManager
import com.raphtory.core.analysis.Managers.LiveManagers.{BWindowedLiveAnalysisManager, LiveAnalysisManager, WindowedLiveAnalysisManager}
import com.raphtory.core.analysis.Managers.RangeManagers.{BWindowedRangeAnalysisManager, RangeAnalysisManager, WindowedRangeAnalysisManager}
import com.raphtory.core.analysis.Managers.ViewManagers.{BWindowedViewAnalysisManager, ViewAnalysisManager, WindowedViewAnalysisManager}

case class LiveAnalysisNode(seedLoc: String, name: String) extends DocSvr {

  implicit val system: ActorSystem = initialiseActorSystem(seeds = List(seedLoc))

  private val jobID = sys.env.getOrElse("JOBID", "Default").toString
  private val analyser = Class.forName(this.name).newInstance().asInstanceOf[Analyser]

  // FIXME: Need to make this neater, extract the type checks into constants,
  //        and have the default fall back strategy better defined
  sys.env.getOrElse("LAMTYPE", "Live").toString match {
    case "Live" => createLiveAnalyst(this.name)
    case "View" => createViewAnalyst(this.name)
    case "Range" => createRangeAnalyst(this.name)
  }

  def createLiveAnalyst(nodeName: String): ActorRef = {
    val analysisManager: AnalysisManager = sys.env.getOrElse("WINDOWTYPE", "false") match {
      case "false" => new LiveAnalysisManager(jobID = jobID, analyser = analyser)
      case "true" => new WindowedLiveAnalysisManager(jobID = jobID, analyser = analyser)
      case "batched" => new BWindowedLiveAnalysisManager(jobID = jobID, analyser = analyser)
    }

    val actorName = s"LiveAnalysisManager_$name"
    system.actorOf(Props(analysisManager), actorName)
  }

  def createViewAnalyst(nodeName: String): ActorRef = {
    val time = sys.env.getOrElse("TIMESTAMP", "0").toLong

    val analysisManager: ViewAnalysisManager = sys.env.getOrElse("WINDOWTYPE", "false") match {
      case "false" =>
        new ViewAnalysisManager(jobID = jobID, analyser = analyser, time = time)
      case "true" =>
        val window = sys.env.getOrElse("WINDOW", "0").toLong
        new WindowedViewAnalysisManager(jobID = jobID, analyser = analyser, time = time, window = window)
      case "batched" =>
        val windowSet = sys.env.getOrElse("WINDOWSET", "0").split(",").map(f => f.toLong)
        new BWindowedViewAnalysisManager(jobID = jobID, analyser = analyser, time = time, windows = windowSet)
    }

    val actorName = s"ViewAnalysisManager_$name"
    system.actorOf(Props(analysisManager), actorName)
  }

  def createRangeAnalyst(nodeName: String): ActorRef = {
    val start = sys.env.getOrElse("START", "0").toLong
    val end = sys.env.getOrElse("END", "0").toLong
    val jump = sys.env.getOrElse("JUMP", "0").toLong

    val analysisManager: RangeAnalysisManager = sys.env.getOrElse("WINDOWTYPE", "false") match {
      case "false" =>
        new RangeAnalysisManager(jobID = jobID, analyser = analyser, start = start, end = end, jump = jump)
      case "true" =>
        val window = sys.env.getOrElse("WINDOW", "0").toLong
        new WindowedRangeAnalysisManager(jobID = jobID, analyser = analyser, start = start, end = end, jump = jump, window = window)
      case "batched" =>
        val windowSet = sys.env.getOrElse("WINDOWSET", "0").split(",").map(f => f.toLong)
        new BWindowedRangeAnalysisManager(jobID = jobID, analyser = analyser, start = start, end = end, jump = jump, windows = windowSet)
    }

    val actorName = s"RangeAnalysisManager_$name"
    system.actorOf(Props(analysisManager), actorName)
  }
}
