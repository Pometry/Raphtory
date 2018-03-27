package com.raphtory.caseclass.clustercase

import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, _}
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import com.raphtory.Actors.ClusterActors.DocSvr
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._


case class RestNode(seedLoc : String) extends DocSvr {
	import akka.cluster.pubsub._

	implicit val system = init(List(seedLoc))
	implicit val materializer = ActorMaterializer()
	implicit val t:Timeout = 15.seconds

	system.actorOf(Props(new ClusterActor(this)), "cluster")

	val port  = ConfigFactory.load().getInt("settings.http")
	val iface = java.net.InetAddress.getLocalHost().getHostAddress()
	val mediator = DistributedPubSub(system).mediator

	val requestHandler: HttpRequest ⇒ HttpResponse = {
		case HttpRequest(GET, Uri.Path("/nodes"), _, _, _)  => HttpResponse(entity = s"""{"nnodes":"[${getNodes()}]}""")

    //submit your own command
    case HttpRequest(POST,Uri.Path("/command"),_,entity,_)  => {
			println("Sending data to Router")
      val value = entity.toStrict(5 seconds).map(_.data.decodeString("UTF-8"))
      value.onComplete(f=>{
        mediator ! DistributedPubSubMediator.Send("/user/router",f.get,false)
      })
      HttpResponse(entity = s"""sending data to manager 1""")
		}
    case HttpRequest(GET, Uri.Path("/addvertex"),_,_,_)  => {
      println("Sending add vertex to router")
      val resp = Await.result(mediator ? DistributedPubSubMediator.Send("/user/updateGen","addVertex",false), t.duration).asInstanceOf[String]
      HttpResponse(entity = s"Command Generated: $resp")
    }

    case HttpRequest(GET, Uri.Path("/rmvvertex"),_,_,_)  => {
      println("Sending remove vertex to router")
      val resp = Await.result(mediator ? DistributedPubSubMediator.Send("/user/updateGen","removeVertex",false), t.duration).asInstanceOf[String]
      HttpResponse(entity = s"Command Generated: $resp")
    }
    case HttpRequest(GET, Uri.Path("/addedge"),_,_,_)  => {
      println("Sending add edge to router")
      val resp = Await.result(mediator ? DistributedPubSubMediator.Send("/user/updateGen","addEdge",false), t.duration).asInstanceOf[String]
      HttpResponse(entity = s"Command Generated: $resp")
    }
    case HttpRequest(GET, Uri.Path("/rmvedge"),_,_,_)  => {
      println("Sending remove edge to router")
      val resp = Await.result(mediator ? DistributedPubSubMediator.Send("/user/updateGen","removeEdge",false), t.duration).asInstanceOf[String]
      HttpResponse(entity = s"Command Generated: $resp")
    }
    case HttpRequest(GET, Uri.Path("/random"),_,_,_)  => {
      println("Sending 10 random commands to router")
      val resp = Await.result(mediator ? DistributedPubSubMediator.Send("/user/updateGen","random",false), t.duration).asInstanceOf[String]
      HttpResponse(entity = s"Command Generated: $resp")
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
