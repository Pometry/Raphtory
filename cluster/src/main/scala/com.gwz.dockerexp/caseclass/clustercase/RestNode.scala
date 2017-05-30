package com.gwz.dockerexp.caseclass.clustercase

import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout
import com.gwz.dockerexp.Actors.ClusterActors.{ClusterActor, DocSvr}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

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
		case HttpRequest(GET, Uri.Path("/nodes"), _, _, _)  => HttpResponse(entity = s"""{"nodes":"[${getNodes()}]}""")

		case HttpRequest(GET, Uri.Path("/svc"), _, _, _)  => {
			println("Making logic call with AKKA...")
			val resp = Await.result(mediator ? DistributedPubSubMediator.Send("/user/logic","hey",false), t.duration).asInstanceOf[String]
			HttpResponse(entity = s"""{"cluster_node":"$resp"}""")
		}

		case _: HttpRequest => HttpResponse(404, entity = "Unknown resource!")
	}

	val serverSource: Source[Http.IncomingConnection, Future[Http.ServerBinding]] =
		Http(system).bind(interface = iface, port = port)
	val bindingFuture: Future[Http.ServerBinding] = serverSource.to(Sink.foreach { connection =>
		connection handleWithSyncHandler requestHandler
	}).run()
}
