package com.raphtory.core.clustersetup

/**
  * Created by Mirate on 29/09/2017.
  */
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Tasks.AnalysisTask
import com.raphtory.core.analysis.Tasks.LiveTasks.BWindowedLiveAnalysisTask
import com.raphtory.core.analysis.Tasks.LiveTasks.LiveAnalysisTask
import com.raphtory.core.analysis.Tasks.LiveTasks.WindowedLiveAnalysisTask
import com.raphtory.core.analysis.Tasks.RangeTasks.BWindowedRangeAnalysisTask
import com.raphtory.core.analysis.Tasks.RangeTasks.RangeAnalysisTask
import com.raphtory.core.analysis.Tasks.RangeTasks.WindowedRangeAnalysisTask
import com.raphtory.core.analysis.Tasks.ViewTasks.BWindowedViewAnalysisTask
import com.raphtory.core.analysis.Tasks.ViewTasks.ViewAnalysisTask
import com.raphtory.core.analysis.Tasks.ViewTasks.WindowedViewAnalysisTask

import akka.actor.Actor
import akka.actor.Props
import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Tasks.RangeTasks.BWindowedRangeAnalysisTask
import com.raphtory.core.analysis.Tasks.RangeTasks.RangeAnalysisTask
import com.raphtory.core.analysis.Tasks.RangeTasks.WindowedRangeAnalysisTask
import com.raphtory.core.analysis.Tasks.ViewTasks.BWindowedViewAnalysisTask
import com.raphtory.core.analysis.Tasks.ViewTasks.ViewAnalysisTask
import com.raphtory.core.analysis.Tasks.ViewTasks.WindowedViewAnalysisTask
import com.raphtory.core.analysis.Tasks.LiveTasks.BWindowedLiveAnalysisTask
import com.raphtory.core.analysis.Tasks.LiveTasks.LiveAnalysisTask
import com.raphtory.core.analysis.Tasks.LiveTasks.WindowedLiveAnalysisTask
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




