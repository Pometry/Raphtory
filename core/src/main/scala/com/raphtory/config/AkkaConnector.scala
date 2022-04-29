package com.raphtory.config

import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Props
import akka.actor.typed.Scheduler
import akka.actor.typed.SpawnProtocol
import akka.util.Timeout

import java.util.concurrent.CompletableFuture
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.reflect.ClassTag

class AkkaConnector(actorSystem: ActorSystem[SpawnProtocol.Command]) extends Connector {
  val akkaReceptionistRegisteringTimeout: Timeout = 1.seconds
  val akkaSpawnerTimeout: Timeout                 = 1.seconds
  val akkaReceptionistFindingTimeout: Timeout     = 1.seconds

  case class AkkaEndPoint[T](actorRefSet: Future[Set[ActorRef[T]]]) extends EndPoint[T] {
    implicit val endPointResolutionTimeout: Duration = 1.seconds

    override def sendAsync(message: T): Unit           =
      Await.result(actorRefSet, endPointResolutionTimeout) foreach (_ ! message)
    override def close(): Unit = {}

    override def flushAsync(): CompletableFuture[Void] = CompletableFuture.completedFuture(null)
    override def closeWithMessage(message: T): Unit    = sendAsync(message)
  }

  override def endPoint[T](topic: CanonicalTopic[T]): EndPoint[T] = {
    val serviceKey                    = topic match {
      case ExclusiveTopic(_, id, subTopic) => getServiceKey(topic)
      case BroadcastTopic(_, id, subTopic) => getServiceKey(topic)
//      case WorkPullTopic(connector, id, subTopic) =>
//        throw new Exception("Work pull topics cannot be handled by Akka yet")
    }
    implicit val scheduler: Scheduler = actorSystem.scheduler
    implicit val timeout: Timeout     = akkaReceptionistFindingTimeout
    val listing                       = actorSystem.receptionist ? Receptionist.Find(serviceKey)
    implicit val ec                   = actorSystem.executionContext
    val instance                      = listing.map(_.serviceInstances(serviceKey)) // TODO: unsafe
    AkkaEndPoint(instance)
  }

  override def register[T](
      id: String,
      messageHandler: T => Unit,
      topics: Seq[CanonicalTopic[T]]
  ): CancelableListener = {
    val behavior = Behaviors.setup[T] { context =>
      implicit val timeout: Timeout     = akkaReceptionistRegisteringTimeout
      implicit val scheduler: Scheduler = context.system.scheduler
      topics foreach { topic =>
        context.system.receptionist ! Receptionist.Register(getServiceKey(topic), context.self)
      }
      Behaviors.receiveMessage[T] { message =>
        messageHandler.apply(message)
        Behaviors.same
      }
    }
    new CancelableListener {
      override def start(): Unit = {
        implicit val timeout: Timeout     = akkaSpawnerTimeout
        implicit val scheduler: Scheduler = actorSystem.scheduler
        actorSystem ? ((ref: ActorRef[ActorRef[T]]) =>
          SpawnProtocol.Spawn(behavior, id, Props.empty, ref)
        )
      }

      override def close(): Unit = {} // TODO: there should be a way to deregister the actor
    }
  }

  private def getServiceKey[T](topic: Topic[T]) =
    ServiceKey[T](s"${topic.id}-${topic.subTopic}")
}
