package com.raphtory.core.analysis

import akka.actor.ActorSystem
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
import akka.cluster.pubsub._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import com.raphtory.core.clustersetup.util.ConfigUtils.SystemConfig
import com.raphtory.core.model.communication.{AnalysisRequest, LiveAnalysisRequest, RangeAnalysisRequest, ViewAnalysisRequest}
import spray.json.DefaultJsonProtocol._

import scala.concurrent.Future
case class LiveAnalysisPOST(jobID:String, analyserName:String, windowType:Option[String], windowSize:Option[Long], windowSet:Option[Array[Long]])
case class ViewAnalysisPOST(jobID:String,analyserName:String,timestamp:Long,windowType:Option[String],windowSize:Option[Long],windowSet:Option[Array[Long]])
case class RangeAnalysisPOST(jobID:String,analyserName:String,start:Long,end:Long,jump:Long,windowType:Option[String],windowSize:Option[Long],windowSet:Option[Array[Long]]) extends AnalysisRequest
case class AnalysisRestApi(system:ActorSystem){
  println("running 2")
  implicit val system2 = system
  implicit val materializer = ActorMaterializer()
  implicit val t:Timeout = 15.seconds
  val port  = 8081
  var config = ConfigFactory.load().getString("akka.remote.netty.tcp.bind-hostname")
  val iface = config//java.net.InetAddress.getLocalHost().getHostAddress()
  println(iface)
  val mediator = DistributedPubSub(system).mediator

  val requestHandler: HttpRequest ⇒ HttpResponse = {
    //Submit Analysis
    case HttpRequest(POST,Uri.Path("/LiveAnalysisRequest"),_,entity,_)  => {
      try{
        implicit val LiveAnalysisFormat = jsonFormat5(LiveAnalysisPOST)
        val in:LiveAnalysisPOST = Await.result(Unmarshal(entity).to[LiveAnalysisPOST], 10.second)
        val response = LiveAnalysisRequest(in.jobID,in.analyserName,in.windowType.getOrElse("false"),in.windowSize.getOrElse(0),in.windowSet.getOrElse(Array()))
        mediator ! DistributedPubSubMediator.Send("/user/AnalysisManager", response, false)
        HttpResponse(entity = s"""Your Task ${in.jobID} Has been successfully submitted as a Live Analysis Task!""")
      }
      catch {case e:Exception => e.printStackTrace();HttpResponse(entity = "Your Task Appeared to have some issue, please check your JSON and resubmit")}
    }
    case HttpRequest(POST,Uri.Path("/ViewAnalysisRequest"),_,entity,_)  => {
      try{
        implicit val viewAnalysisPOST = jsonFormat6(ViewAnalysisPOST)
        val in:ViewAnalysisPOST = Await.result(Unmarshal(entity).to[ViewAnalysisPOST], 10.second)
        val response = ViewAnalysisRequest(in.jobID,in.analyserName,in.timestamp,in.windowType.getOrElse("false"),in.windowSize.getOrElse(0),in.windowSet.getOrElse(Array()))
        mediator ! DistributedPubSubMediator.Send("/user/AnalysisManager", response, false)
        HttpResponse(entity = s"""Your Task ${in.jobID} Has been successfully submitted as a View Analysis Task!""")
      }
      catch {case e:Exception => e.printStackTrace();HttpResponse(entity = "Your Task Appeared to have some issue, please check your JSON and resubmit")}
    }
    case HttpRequest(POST,Uri.Path("/RangeAnalysisRequest"),_,entity,_)  => {
      try{
        implicit val rangeAnalysisPOST = jsonFormat8(RangeAnalysisPOST)
        val in:RangeAnalysisPOST = Await.result(Unmarshal(entity).to[RangeAnalysisPOST], 10.second)
        val response = RangeAnalysisRequest(in.jobID,in.analyserName,in.start,in.end,in.jump,in.windowType.getOrElse("false"),in.windowSize.getOrElse(0),in.windowSet.getOrElse(Array()))
        mediator ! DistributedPubSubMediator.Send("/user/AnalysisManager", response, false)
        HttpResponse(entity = s"""Your Task ${in.jobID} Has been successfully submitted as a Range Analysis Task!""")
      }
      catch {case e:Exception => e.printStackTrace();HttpResponse(entity = "Your Task Appeared to have some issue, please check your JSON and resubmit")}
    }

    case last: HttpRequest => {
      HttpResponse(404, entity = s"unknown address")
    }
  }
  val serverSource: Source[Http.IncomingConnection, Future[Http.ServerBinding]] =
    Http(system).bind(interface = iface, port = port)
  val bindingFuture: Future[Http.ServerBinding] = serverSource.to(Sink.foreach { connection =>
    connection handleWithSyncHandler requestHandler
  }).run()




}