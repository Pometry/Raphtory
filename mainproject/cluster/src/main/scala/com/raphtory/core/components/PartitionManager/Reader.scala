package com.raphtory.core.components.PartitionManager

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.Terminated
import akka.cluster.pubsub.DistributedPubSubMediator.SubscribeAck
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.components.PartitionManager.Workers.ReaderWorker
import com.raphtory.core.model.communication._
import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.Utils
import com.twitter.util.Eval

import scala.collection.parallel.mutable.ParTrieMap
import scala.util.Try

class Reader(
    id: Int,
    test: Boolean,
    managerCountVal: Int,
    storage: ParTrieMap[Int, EntityStorage],
    workerCount: Int = 10
) extends Actor
        with ActorLogging {

  implicit var managerCount: Int = managerCountVal

  // Id which refers to the partitions position in the graph manager map
  val managerId: Int = id

  val mediator: ActorRef = DistributedPubSub(context.system).mediator

  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.readersTopic, self)

  var readers: ParTrieMap[Int, ActorRef] = new ParTrieMap[Int, ActorRef]()

  for (i <- 0 until workerCount) {
    log.debug("Initialising [{}] worker children for Reader [{}}.", workerCount, managerId)

    // create threads for writing
    val child = context.system.actorOf(
            Props(new ReaderWorker(managerCount, managerId, i, storage(i))).withDispatcher("reader-dispatcher"),
            s"Manager_${id}_reader_$i"
    )

    context.watch(child)
    readers.put(i, child)
  }

  override def preStart(): Unit =
    log.debug("Reader [{}] is being started.", managerId)

  override def receive: Receive = {
    case ReaderWorkersOnline()     => sender ! ReaderWorkersACK()
    case req: AnalyserPresentCheck => processAnalyserPresentCheckRequest(req)
    case req: TimeCheck            => processTimeCheckRequest(req)
    case req: CompileNewAnalyser   => processCompileNewAnalyserRequest(req)
    case req: UpdatedCounter       => processUpdatedCounterRequest(req)
    case SubscribeAck              =>
    case Terminated(child) =>
      log.warning(s"ReaderWorker with path [{}] belonging to Reader [{}] has died.", child.path, managerId)
    case x => log.warning(s"Reader [{}] received unknown [{}] message.", managerId, x)
  }

  def processAnalyserPresentCheckRequest(req: AnalyserPresentCheck): Unit = {
    log.debug(s"Reader [{}] received [{}] request.", managerId, req)

    val className   = req.className
    val classExists = Try(Class.forName(className))

    classExists.toEither.fold(
            { _: Throwable =>
              log.debug("Class [{}] was not found within this image.", className)

              sender ! ClassMissing()
            }, { _: Class[_] =>
              log.debug(s"Class [{}] exists. Proceeding.", className)

              sender ! AnalyserPresent()
            }
    )
  }

  def processTimeCheckRequest(req: TimeCheck): Unit = {
    log.debug(s"Reader [{}] received [{}] request.", managerId, req)

    val timestamp = req.timestamp

    val newest = storage.map { case (_, entityStorage) => entityStorage.newestTime }.max

    if (timestamp <= newest) {
      log.debug("Received timestamp is smaller or equal to newest entityStorage timestamp.")

      sender ! TimeResponse(ok = true, newest)
    } else {
      log.debug("Received timestamp is larger than newest entityStorage timestamp.")

      sender ! TimeResponse(ok = false, newest)
    }
  }

  def processCompileNewAnalyserRequest(req: CompileNewAnalyser): Unit = {
    log.debug("Reader [{}] received [{}] request.", managerId, req)

    val (analyserString, name) = (req.analyser, req.name)

    log.debug("Compiling [{}] for LAM.", name)

    val evalResult = Try {
      val eval               = new Eval
      val analyser: Analyser = eval[Analyser](analyserString)
      Utils.analyserMap += ((name, analyser))
    }

    evalResult.toEither.fold(
            { t: Throwable =>
              log.debug("Compilation of [{}] failed due to [{}].", name, t)

              sender ! ClassMissing()
            }, { _ =>
              log.debug(s"Compilation of [{}] succeeded. Proceeding.", name)

              sender ! ClassCompiled()
            }
    )
  }

  def processUpdatedCounterRequest(req: UpdatedCounter): Unit = {
    log.debug("Reader [{}] received [{}] request.", managerId, req)

    managerCount = req.newValue
    readers.foreach(x => x._2 ! UpdatedCounter(req.newValue))
  }
}
