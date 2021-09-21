package com.raphtory.core.actors.analysismanager

import akka.actor._
import akka.cluster.pubsub._
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.raphtory.core.actors.analysismanager.AnalysisManager.Message._
import com.raphtory.core.actors.analysismanager.AnalysisRestApi.http._
import com.raphtory.core.actors.analysismanager.AnalysisRestApi.message._
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

final case class AnalysisRestApi(system: ActorSystem) {
  implicit val systemImpl: ActorSystem = system
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ex: ExecutionContext = system.dispatcher
  implicit val timeout: Timeout = 15.seconds
  private val port              = 8081
  private val iface             = "0.0.0.0" //config//java.net.InetAddress.getLocalHost().getHostAddress()
  private val mediator          = DistributedPubSub(system).mediator

  val route: Route = {
    (path("LiveAnalysisRequest") & post) {
        entity(as[LiveAnalysisPost]) { in =>
          val request = LiveAnalysisRequest(
            in.analyserName,
            in.serialiserName,
            in.repeatTime.getOrElse(0),
            in.eventTime.getOrElse(false),
            in.windowSet.getOrElse(List.empty),
            in.args.getOrElse(Array())
          )
          mediator ! new DistributedPubSubMediator.Send("/user/AnalysisManager", request)
          complete(s"Your Task Has been successfully submitted as a Live Analysis Task!")
        }
    } ~ (path("ViewAnalysisRequest") & post) {
      entity(as[ViewAnalysisPost]) { in =>
        val request = ViewAnalysisRequest(
          in.analyserName,
          in.serialiserName,
          in.timestamp,
          in.windowSet.getOrElse(List.empty),
          in.args.getOrElse(Array())
        )
        mediator ! new DistributedPubSubMediator.Send("/user/AnalysisManager", request)
        complete(s"Your Task Has been successfully submitted as a View Analysis Task!")
      }
    } ~ (path("RangeAnalysisRequest") & post) {
      entity(as[RangeAnalysisPost]) { in =>
        val request = RangeAnalysisRequest(
          in.analyserName,
          in.serialiserName,
          in.start,
          in.end,
          in.jump,
          in.windowSet.getOrElse(List.empty),
          in.args.getOrElse(Array())
        )
        mediator ! new DistributedPubSubMediator.Send("/user/AnalysisManager", request)
        complete(s"Your Task Has been successfully submitted as a Range Analysis Task!")
      }
    }
  } ~ (path("AnalysisResults") & get & parameter("jobID")) { jobId =>
   val result =  (mediator ? new DistributedPubSubMediator.Send("/user/AnalysisManager", RequestResults(jobId))).map {
      case ResultsForApiPI(results) => results.mkString("{results:[", ",", "]}")
      case JobDoesntExist           => "JobID given doesn't exist"
    }
    complete(result)
  } ~ (path("KillTask") & get & parameter("jobID")) { jobId =>
    val result =  (mediator ? new DistributedPubSubMediator.Send("/user/AnalysisManager", KillTask(jobId))).map {
      case JobKilled      => s"Analysis has been stopped for $jobId"
      case JobDoesntExist => "JobID given doesn't exist"
    }
    complete(result)
  }

  Http().bindAndHandle(route, iface, port)
}

object AnalysisRestApi {
  object http {
    case class LiveAnalysisPost(
        analyserName: String,
        serialiserName:String,
        windowSet: Option[List[Long]],
        repeatTime: Option[Long],
        eventTime: Option[Boolean],
        args: Option[Array[String]],
        rawFile: Option[String]
    )
    object LiveAnalysisPost {
      implicit val formatter: RootJsonFormat[LiveAnalysisPost] = jsonFormat7(LiveAnalysisPost.apply)
    }

    case class ViewAnalysisPost(
        analyserName: String,
        serialiserName:String,
        timestamp: Long,
        windowSet: Option[List[Long]],
        args: Option[Array[String]],
        rawFile: Option[String]
    )
    object ViewAnalysisPost {
      implicit val formatter: RootJsonFormat[ViewAnalysisPost] = jsonFormat6(ViewAnalysisPost.apply)
    }

    case class RangeAnalysisPost(
        analyserName: String,
        serialiserName:String,
        start: Long,
        end: Long,
        jump: Long,
        windowSet: Option[List[Long]],
        args: Option[Array[String]],
        rawFile: Option[String]
    )

    object RangeAnalysisPost {
      implicit val formatter: RootJsonFormat[RangeAnalysisPost] = jsonFormat8(RangeAnalysisPost.apply)
    }
  }

  object message {
    sealed trait AnalysisRequest

    case class LiveAnalysisRequest(
        analyserName: String,
        serialiserName:String,
        repeatTime: Long,
        eventTime: Boolean,
        windowSet: List[Long],
        args: Array[String]
    ) extends AnalysisRequest

    case class ViewAnalysisRequest(
        analyserName: String,
        serialiserName:String,
        timestamp: Long,
        windowSet: List[Long],
        args: Array[String]
    ) extends AnalysisRequest

    case class RangeAnalysisRequest(
        analyserName: String,
        serialiserName:String,
        start: Long,
        end: Long,
        jump: Long,
        windowSet: List[Long],
        args: Array[String]
    ) extends AnalysisRequest
  }

}
