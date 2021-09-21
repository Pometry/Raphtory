package com.raphtory.core.actors.orchestration

import akka.actor.{Actor, ActorLogging, ActorPath, ActorRef, ActorSystem, ClassicActorSystemProvider, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.dispatch._
import com.typesafe.config.Config

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

class SizeTrackedMailbox extends MailboxType with ProducesMessageQueue[SizeTrackedMailbox.MailboxTrackedMessageQueue] {
  import SizeTrackedMailbox._

  def this(settings: ActorSystem.Settings, config: Config) = {
    this()
  }

  final override def create(owner: Option[ActorRef], system: Option[ActorSystem]): MessageQueue =
    (owner, system) match {
      case (Some(o), Some(s)) => new MailboxTrackedMessageQueue(o.path, MailboxTrackingExtension(s))
      case _                  => throw new IllegalArgumentException("no owner or system given")
    }
}

object SizeTrackedMailbox {
  // This is the MessageQueue implementation
  class MailboxTrackedMessageQueue(actorPath: ActorPath, counter: MailboxTrackingExtensionImpl)
          extends UnboundedMailbox.MessageQueue {
    counter.init(actorPath)

    override def enqueue(receiver: ActorRef, handle: Envelope): Unit = {
      super.enqueue(receiver, handle)
      counter.increment(actorPath)
    }

    override def dequeue(): Envelope = {
      val msg = super.dequeue()
      if (msg ne null)
        counter.decrement(actorPath)
      msg
    }
    override def cleanUp(owner: ActorRef, deadLetters: MessageQueue): Unit = {
      super.cleanUp(owner, deadLetters)
      counter.clean(actorPath)
    }
  }
}

class MailboxTrackingExtensionImpl extends Extension {
  private val counters = new ConcurrentHashMap[String, AtomicInteger]()

  def init(path: ActorPath) = counters.put(path.toString, new AtomicInteger(0))

  def increment(path: ActorPath): Unit = counters.get(path.toString).incrementAndGet()
  def decrement(path: ActorPath): Unit = counters.get(path.toString).decrementAndGet()

  def current(path: ActorPath) = counters.get(path.toString).get()
  def clean(path: ActorPath)   = counters.get(path.toString).set(0)
}

object MailboxTrackingExtension extends ExtensionId[MailboxTrackingExtensionImpl] with ExtensionIdProvider {
  override def lookup = MailboxTrackingExtension

  override def createExtension(system: ExtendedActorSystem) = new MailboxTrackingExtensionImpl

  override def get(system: ActorSystem): MailboxTrackingExtensionImpl                = super.get(system)
  override def get(system: ClassicActorSystemProvider): MailboxTrackingExtensionImpl = super.get(system)
}

trait MailboxTrackedActor extends Actor with ActorLogging {
  val mailBoxCounter: MailboxTrackingExtensionImpl = MailboxTrackingExtension(context.system)

  def mailboxTrackedReceive(work: Receive): Receive = {
    case m =>
//      log.info(s"${self.path} mailbox size [${mailBoxCounter.current(self.path)}]")
      work(m)
  }
}
