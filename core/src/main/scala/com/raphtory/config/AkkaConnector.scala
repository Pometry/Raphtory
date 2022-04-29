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
import com.raphtory.serialisers.PulsarKryoSerialiser

import java.util.concurrent.CompletableFuture
import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

class AkkaConnector(actorSystem: ActorSystem[SpawnProtocol.Command]) extends Connector {
  val akkaReceptionistRegisteringTimeout: Timeout = 1.seconds
  val akkaSpawnerTimeout: Timeout                 = 1.seconds
  val akkaReceptionistFindingTimeout: Timeout     = 1.seconds

  val kryo: PulsarKryoSerialiser = PulsarKryoSerialiser()

  case class AkkaEndPoint[T](actorRefs: Future[Set[ActorRef[Array[Byte]]]]) extends EndPoint[T] {
    private val endPointResolutionTimeout: Duration = 10.seconds

    override def sendAsync(message: T): Unit           =
      Await.result(actorRefs, endPointResolutionTimeout) foreach (_ ! serialise(message))
    override def close(): Unit = {}

    override def flushAsync(): CompletableFuture[Void] = CompletableFuture.completedFuture(null)
    override def closeWithMessage(message: T): Unit    = sendAsync(message)
  }

  override def endPoint[T](topic: CanonicalTopic[T]): EndPoint[T] = {
    val (serviceKey, numActors) = topic match {
      case topic: ExclusiveTopic[T] => (getServiceKey(topic), 1)
      case topic: BroadcastTopic[T] => (getServiceKey(topic), topic.numListeners)
      case _: WorkPullTopic[T]      =>
        throw new Exception("Work pull topics cannot be handled by Akka yet")
    }
    implicit val ec             = actorSystem.executionContext
    AkkaEndPoint(Future(queryServiceKeyActors(serviceKey, numActors)))
  }

//  @tailrec
//  private def queryServiceKeyActors(
//      serviceKey: ServiceKey[Any],
//      numActors: Int
//  ): Future[Set[ActorRef[Any]]] = {
//    implicit val scheduler: Scheduler = actorSystem.scheduler
//    implicit val timeout: Timeout     = akkaReceptionistFindingTimeout
//    val listing                       = actorSystem.receptionist ? Receptionist.Find(serviceKey)
//    implicit val ec                   = actorSystem.executionContext
//    val instances                     = listing.map(_.serviceInstances(serviceKey)) // TODO: unsafe
//    instances.flatMap { instances =>
//      if (instances.size == numActors)
//        Future(instances)
//      else if (instances.size > numActors) {
//        val msg =
//          s"${instances.size} instances for service key '${serviceKey.id}', but only $numActors expected"
//        throw new Exception(msg)
//      }
//      else
//        queryServiceKeyActors(serviceKey, numActors)
//    }
//  }

  override def register[T](
      id: String,
      messageHandler: T => Unit,
      topics: Seq[CanonicalTopic[T]]
  ): CancelableListener = {
    val behavior = Behaviors.setup[Array[Byte]] { context =>
      implicit val timeout: Timeout     = akkaReceptionistRegisteringTimeout
      implicit val scheduler: Scheduler = context.system.scheduler
      topics foreach { topic =>
        context.system.receptionist ! Receptionist.Register(getServiceKey(topic), context.self)
      }
      Behaviors.receiveMessage[Array[Byte]] { message =>
        messageHandler.apply(deserialise(message))
        Behaviors.same
      }
    }
    new CancelableListener {
      override def start(): Unit = {
        implicit val timeout: Timeout     = akkaSpawnerTimeout
        implicit val scheduler: Scheduler = actorSystem.scheduler
        actorSystem ? ((ref: ActorRef[ActorRef[Array[Byte]]]) =>
          SpawnProtocol.Spawn(behavior, id, Props.empty, ref)
        )
      }

      override def close(): Unit = {} // TODO: there should be a way to deregister the actor
    }
  }

  @tailrec
  private def queryServiceKeyActors(
      serviceKey: ServiceKey[Array[Byte]],
      numActors: Int
  ): Set[ActorRef[Array[Byte]]] = {
    implicit val scheduler: Scheduler = actorSystem.scheduler
    implicit val timeout: Timeout     = akkaReceptionistFindingTimeout
    val futureListing                 = actorSystem.receptionist ? Receptionist.Find(serviceKey)
    val listing                       = Await.result(futureListing, 1.seconds)
    val instances                     = listing.serviceInstances(serviceKey)

    if (instances.size == numActors)
      instances
    else if (instances.size > numActors) {
      val msg =
        s"${instances.size} instances for service key '${serviceKey.id}', but only $numActors expected"
      throw new Exception(msg)
    }
    else
      queryServiceKeyActors(serviceKey, numActors)
  }

  private def getServiceKey[T](topic: Topic[T]) =
    ServiceKey[Array[Byte]](s"${topic.id}-${topic.subTopic}")

  private def deserialise[T](bytes: Array[Byte]): T = kryo.deserialise[T](bytes)
  private def serialise(value: Any): Array[Byte]    = kryo.serialise(value)
}
