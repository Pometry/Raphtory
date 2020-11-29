package com.raphtory.core.analysis

import akka.actor.{ActorSystem, _}
import akka.cluster.pubsub._
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, _}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import com.raphtory.core.model.communication._
import com.typesafe.config.ConfigFactory
import spray.json.DefaultJsonProtocol._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

case class AnalysisRestApi(system:ActorSystem){
  implicit val system2 = system
  implicit val materializer = ActorMaterializer()
  implicit val t:Timeout = 15.seconds
  val port  = 8081
  var config = ConfigFactory.load().getString("akka.remote.netty.tcp.bind-hostname")
  val iface = "0.0.0.0"//config//java.net.InetAddress.getLocalHost().getHostAddress()
  val mediator = DistributedPubSub(system).mediator

  val requestHandler: HttpRequest ⇒ HttpResponse = {
    //Submit Analysis
    case HttpRequest(POST,Uri.Path("/LiveAnalysisRequest"),_,entity,_)  => {
      try{
        implicit val LiveAnalysisFormat = jsonFormat8(LiveAnalysisPOST)
        val in:LiveAnalysisPOST = Await.result(Unmarshal(entity).to[LiveAnalysisPOST], 10.second)
        val response = LiveAnalysisRequest(in.analyserName,in.repeatTime.getOrElse(0),in.eventTime.getOrElse(false),in.windowType.getOrElse("false"),in.windowSize.getOrElse(0),in.windowSet.getOrElse(Array()),in.args.getOrElse(Array()),in.rawFile.getOrElse(""))
        mediator ! DistributedPubSubMediator.Send("/user/AnalysisManager", response, false)
        HttpResponse(entity = s"""Your Task Has been successfully submitted as a Live Analysis Task!""")
      }
      catch {
        case e:Exception => e.printStackTrace();HttpResponse(entity = "Your Task Appeared to have some issue, please check your JSON and resubmit")}
    }
    case HttpRequest(POST,Uri.Path("/ViewAnalysisRequest"),_,entity,_)  => {
      try{
        implicit val viewAnalysisPOST = jsonFormat7(ViewAnalysisPOST)
        val in:ViewAnalysisPOST = Await.result(Unmarshal(entity).to[ViewAnalysisPOST], 100  .second)
        val response = ViewAnalysisRequest(in.analyserName,in.timestamp,in.windowType.getOrElse("false"),in.windowSize.getOrElse(0),in.windowSet.getOrElse(Array()),in.args.getOrElse(Array()),in.rawFile.getOrElse(""))
        mediator ! DistributedPubSubMediator.Send("/user/AnalysisManager", response, false)
        HttpResponse(entity = s"""Your Task Has been successfully submitted as a View Analysis Task!""")
      }
      catch {case e:Exception => e.printStackTrace();HttpResponse(entity = "Your Task Appeared to have some issue, please check your JSON and resubmit")}
    }
    case HttpRequest(POST,Uri.Path("/RangeAnalysisRequest"),_,entity,_)  => {
      try{
        implicit val rangeAnalysisPOST = jsonFormat9(RangeAnalysisPOST)
        val in:RangeAnalysisPOST = Await.result(Unmarshal(entity).to[RangeAnalysisPOST], 10.second)
        val response = RangeAnalysisRequest(in.analyserName,in.start,in.end,in.jump,in.windowType.getOrElse("false"),in.windowSize.getOrElse(0),in.windowSet.getOrElse(Array()),in.args.getOrElse(Array()),in.rawFile.getOrElse(""))
        mediator ! DistributedPubSubMediator.Send("/user/AnalysisManager", response, false)
        HttpResponse(entity = s"""Your Task Has been successfully submitted as a Range Analysis Task!""")
      }
      catch {case e:Exception => e.printStackTrace();HttpResponse(entity = "Your Task Appeared to have some issue, please check your JSON and resubmit")}
    }
    //get results
    case HttpRequest(GET,uri,_,entity,_)  => {
      uri.path.toString() match {
        case "/AnalysisResults" => analysisResults(uri)
        case "/KillTask" => killTask(uri)
        case _ => fourOhFour(uri)
      }

    }
    case last: HttpRequest => {
      HttpResponse(404, entity = s"unknown address")
    }
  }

  def analysisResults(uri: Uri) = {

    uri.rawQueryString match {
      case Some(queries) =>
        val querySplit = queries.split("=")
        if(querySplit.size==2 && querySplit(0).equals("jobID")) {
          try {
            val future = mediator ? DistributedPubSubMediator.Send ("/user/AnalysisManager", RequestResults (querySplit(1): String), localAffinity = false)
            Await.result(future, t.duration) match {
              case ResultsForApiPI(results) => outputResults(results)
              case JobDoesntExist() =>  HttpResponse (entity = s"""JobID given doesn't exist""")
            }
          } catch {
            case _: java.util.concurrent.TimeoutException => HttpResponse (entity = s"""Request timed out""")
          }
        }
        else HttpResponse (entity = s"""Please give only the jobID """)
      case None => HttpResponse (entity = s"""Please give a jobID """)
    }
  }

  def outputResults(results: Array[String]) = {
    var output = "{results:["
    results.foreach(result => output+=result)
    output+="]}"
    HttpResponse (entity = output)
  }

  def fourOhFour(uri: Uri) = {HttpResponse(404, entity = s"unknown address")}

  def killTask(uri: Uri)={
    uri.rawQueryString match {
      case Some(queries) =>
        val querySplit = queries.split("=")
        if(querySplit.size==2 && querySplit(0).equals("jobID")) {
          try {
            val future = mediator ? DistributedPubSubMediator.Send ("/user/AnalysisManager", KillTask(querySplit(1): String), localAffinity = false)
            Await.result(future, t.duration) match {
              case JobKilled() => HttpResponse (entity = s"""Analysis has been stopped for ${querySplit(1)}""")
              case JobDoesntExist() =>  HttpResponse (entity = s"""JobID given doesn't exist""")
            }
          } catch {
            case _: java.util.concurrent.TimeoutException => HttpResponse (entity = s"""Request timed out""")
          }
        }
        else HttpResponse (entity = s"""Please give only the jobID """)
      case None => HttpResponse (entity = s"""Please give a jobID """)
    }
  }


  val serverSource: Source[Http.IncomingConnection, Future[Http.ServerBinding]] =
    Http(system).bind(interface = iface, port = port)
  val bindingFuture: Future[Http.ServerBinding] = serverSource.to(Sink.foreach { connection =>
    connection handleWithSyncHandler requestHandler
  }).run()




}