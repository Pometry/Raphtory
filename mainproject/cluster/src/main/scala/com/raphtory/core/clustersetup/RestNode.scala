package com.raphtory.core.clustersetup

import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model._
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.Future

// TODO: Eliminate use of await
// TODO: Change nested futures to future.pipeTo(sender) syntax
case class RestNode(seedLoc: String) extends DocSvr with ActorLogging {

  import akka.cluster.pubsub._

  implicit val system: ActorSystem = initialiseActorSystem(seeds = List(seedLoc))
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val timeout: Timeout = 15.seconds

  final val port = ConfigFactory.load().getInt("settings.http")
  final val inetAddress = java.net.InetAddress.getLocalHost.getHostAddress
  final val mediator = DistributedPubSub(system).mediator

  val requestHandler: HttpRequest ⇒ HttpResponse = {
    case HttpRequest(GET, Uri.Path("/nodes"), _, _, _) =>
      handleNodesRequest()
    case HttpRequest(POST, Uri.Path("/command"), _, entity, _) =>
      handleCommandRequest(entity)
    case HttpRequest(GET, Uri.Path("/addvertex"), _, _, _) =>
      handleAddVertexRequest()
    case HttpRequest(GET, Uri.Path("/rmvvertex"), _, _, _) =>
     handleRmvVertexRequest()
    case HttpRequest(GET, Uri.Path("/addedge"), _, _, _) =>
      handleAddEdgeRequest()
    case HttpRequest(GET, Uri.Path("/rmvedge"), _, _, _) =>
      handleRmvEdgeRequest()
    case HttpRequest(GET, Uri.Path("/random"), _, _, _) =>
      handleRandomRequest()
    case _: HttpRequest =>
      HttpResponse(404, entity = s"Not found.")
  }

  def handleNodesRequest(): HttpResponse = {
    val nodes = getNodes()
    HttpResponse(entity = s"""{"nnodes":"[$nodes]}""")
  }

  def handleCommandRequest(entity: RequestEntity): HttpResponse = {
    log.info("Sending data to Router.")

    val decodedData = entity
      .toStrict(5.seconds)
      .map(strictEntity => strictEntity.data.decodeString("UTF-8"))

    decodedData.onComplete { tryString =>
      val message = DistributedPubSubMediator.Send("/user/router", tryString.get, localAffinity = false)
      mediator ! message
    }

    HttpResponse(entity = s"""Sending data to manager 1""")
  }

  def handleAddVertexRequest(): HttpResponse = {
    log.info("Sending add vertex to router.")

    val action = mediator ? DistributedPubSubMediator.Send("/user/updateGen", "addVertex", localAffinity = false)
    val resp = Await.result(action, timeout.duration).asInstanceOf[String]

    HttpResponse(entity = s"Command Generated: $resp")
  }

  def handleRmvVertexRequest(): HttpResponse = {
    log.info("Sending remove vertex to router")

    val action = mediator ? DistributedPubSubMediator.Send("/user/updateGen", "removeVertex", localAffinity = false)
    val resp = Await.result(mediator ? action, timeout.duration).asInstanceOf[String]

    HttpResponse(entity = s"Command Generated: $resp")
  }

  def handleAddEdgeRequest(): HttpResponse = {
    log.info("Sending add edge to router")

    val action = DistributedPubSubMediator.Send("/user/updateGen", "addEdge", localAffinity = false)
    val resp = Await.result(mediator ? action, timeout.duration).asInstanceOf[String]

    HttpResponse(entity = s"Command Generated: $resp")
  }

  def handleRmvEdgeRequest(): HttpResponse = {
    log.info("Sending remove edge to router")

    val action = DistributedPubSubMediator.Send("/user/updateGen", "removeEdge", localAffinity = false)
    val resp = Await.result(mediator ? action, timeout.duration).asInstanceOf[String]

    HttpResponse(entity = s"Command Generated: $resp")
  }

  def handleRandomRequest(): HttpResponse = {
    log.info("Sending 10 random commands to router")

    val action = DistributedPubSubMediator.Send("/user/updateGen", "random", localAffinity = false)
    val resp = Await.result(mediator ? action, timeout.duration).asInstanceOf[String]

    HttpResponse(entity = s"Command Generated: $resp")
  }

  val serverSource: Source[Http.IncomingConnection, Future[Http.ServerBinding]] =
    Http(system).bind(interface = inetAddress, port = port)

  val bindingFuture: Future[Http.ServerBinding] = serverSource.to(Sink.foreach { connection =>
    connection handleWithSyncHandler requestHandler
  }).run()
}
