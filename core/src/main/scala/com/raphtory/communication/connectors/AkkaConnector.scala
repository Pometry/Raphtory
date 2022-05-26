package com.raphtory.communication.connectors

import akka.actor.typed._
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout
import com.raphtory.communication._
import com.raphtory.serialisers.KryoSerialiser
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.CompletableFuture
import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

/** @DoNotDocument */
case object StopActor

class AkkaConnector(actorSystem: ActorSystem[SpawnProtocol.Command]) extends Connector {
  private val logger: Logger                              = Logger(LoggerFactory.getLogger(this.getClass))
  private val akkaReceptionistRegisteringTimeout: Timeout = 1.seconds
  private val akkaSpawnerTimeout: Timeout                 = 1.seconds
  private val akkaReceptionistFindingTimeout: Timeout     = 1.seconds

  private val kryo: KryoSerialiser = KryoSerialiser()

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
        throw new Exception("work pull topics are not supported by Akka connector")
    }
    implicit val ec             = actorSystem.executionContext
    AkkaEndPoint(Future(queryServiceKeyActors(serviceKey, numActors)))
  }

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
        logger.trace(s"Processing message by component $id")
        val nextBehavior =
          try {
            val value = deserialise[Any](message)
            if (value.isInstanceOf[StopActor.type]) {
              logger.debug(s"Closing akka listener for component $id")
              Behaviors.stopped[Array[Byte]]
            }
            else {
              messageHandler.apply(value.asInstanceOf[T])
              Behaviors.same[Array[Byte]]
            }
          }
          catch {
            case e: Exception =>
              e.printStackTrace()
              logger.error(s"Component $id: Failed to handle message. ${e.getMessage}")
              throw e
          }
          finally Behaviors.same[Array[Byte]]

        nextBehavior
      }
    }
    new CancelableListener {
      var futureSelf: Future[ActorRef[Array[Byte]]] = _
      override def start(): Unit = {
        implicit val timeout: Timeout     = akkaSpawnerTimeout
        implicit val scheduler: Scheduler = actorSystem.scheduler
        futureSelf = actorSystem ? ((ref: ActorRef[ActorRef[Array[Byte]]]) =>
          SpawnProtocol.Spawn(behavior, id, Props.empty, ref)
        )
      }

      override def close(): Unit = {
        val selfResolutionTimeout: Duration = 10.seconds
        Await.result(futureSelf, selfResolutionTimeout) ! serialise(StopActor)
      }
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

  override def shutdown(): Unit = actorSystem.terminate()
}
