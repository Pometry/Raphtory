package com.raphtory.core.clustersetup

/**
  * Created by Mirate on 29/09/2017.
  */
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Managers.AnalysisTask
import com.raphtory.core.analysis.Managers.LiveTasks.BWindowedLiveAnalysisTask
import com.raphtory.core.analysis.Managers.LiveTasks.LiveAnalysisTask
import com.raphtory.core.analysis.Managers.LiveTasks.WindowedLiveAnalysisTask
import com.raphtory.core.analysis.Managers.RangeTasks.BWindowedRangeAnalysisTask
import com.raphtory.core.analysis.Managers.RangeTasks.RangeAnalysisTask
import com.raphtory.core.analysis.Managers.RangeTasks.WindowedRangeAnalysisTask
import com.raphtory.core.analysis.Managers.ViewTasks.BWindowedViewAnalysisTask
import com.raphtory.core.analysis.Managers.ViewTasks.ViewAnalysisTask
import com.raphtory.core.analysis.Managers.ViewTasks.WindowedViewAnalysisTask

import akka.actor.Actor
import akka.actor.Props
import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Managers.RangeTasks.BWindowedRangeAnalysisTask
import com.raphtory.core.analysis.Managers.RangeTasks.RangeAnalysisTask
import com.raphtory.core.analysis.Managers.RangeTasks.WindowedRangeAnalysisTask
import com.raphtory.core.analysis.Managers.ViewTasks.BWindowedViewAnalysisTask
import com.raphtory.core.analysis.Managers.ViewTasks.ViewAnalysisTask
import com.raphtory.core.analysis.Managers.ViewTasks.WindowedViewAnalysisTask
import com.raphtory.core.analysis.Managers.LiveTasks.BWindowedLiveAnalysisTask
import com.raphtory.core.analysis.Managers.LiveTasks.LiveAnalysisTask
import com.raphtory.core.analysis.Managers.LiveTasks.WindowedLiveAnalysisTask
import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, _}
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

case class LiveAnalysisNode(seedLoc: String, name: String) extends DocSvr {
  implicit val system = initialiseActorSystem(List(seedLoc))
}




