package com.raphtory.core.components

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Cancellable, Timers}
import com.raphtory.core.components.RaphtoryActor.{builderServers, buildersPerServer, partitionServers, partitionsPerServer, totalBuilders, totalPartitions}
import com.raphtory.core.model.algorithm.Analyser
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

object RaphtoryActor {
  private val conf = ConfigFactory.load()
  val partitionServers      : Int = conf.getInt("Raphtory.partitionServers")
  val builderServers        : Int = conf.getInt("Raphtory.builderServers")
  val partitionsPerServer   : Int = conf.getInt("Raphtory.partitionsPerServer")
  val buildersPerServer     : Int = conf.getInt("Raphtory.buildersPerServer")
  val spoutCount            : Int = 1
  val analysisCount         : Int = 1
  val totalPartitions       : Int = partitionServers*partitionsPerServer
  val totalBuilders         : Int = builderServers*buildersPerServer
}

trait RaphtoryActor extends Actor with ActorLogging with Timers {

  private lazy val builders     :Array[String]    = (for (i <- 0 until totalBuilders)    yield           s"/user/build_$i"   ).toArray
  private lazy val pms          :Array[String]    = (for (i <- 0 until partitionServers) yield           s"/user/Manager_$i" ).toArray
  private lazy val readers      :Array[String]    = (for (i <- 0 until totalPartitions)  yield           s"/user/read_$i"    ).toArray
  private lazy val writerA      :Array[String]    = (for (i <- 0 until totalPartitions)  yield           s"/user/write_$i"   ).toArray
  private lazy val writerM      :Map[Long,String] = (for (i <- 0 until totalPartitions)  yield (i.toLong,s"/user/write_$i")  ).toMap

  def getAllGraphBuilders()     : Array[String] = builders
  def getAllPartitionManagers() : Array[String] = pms
  def getAllReaders()           : Array[String] = readers
  def getAllWriters()           : Array[String] = writerA
  def getWriter(srcId:Long)     : String        = writerM(srcId.abs % totalPartitions)

  def loadPredefinedAnalyser(className: String, args: Array[String]): Try[Analyser[Any]] =
    Try(Class.forName(className).getConstructor(classOf[Array[String]]).newInstance(args).asInstanceOf[Analyser[Any]])
      .orElse(Try(Class.forName(className).getConstructor().newInstance().asInstanceOf[Analyser[Any]]))

  def scheduleTask(initialDelay: FiniteDuration, interval: FiniteDuration, receiver: ActorRef, message: Any)
                  (implicit context: ActorContext, executor: ExecutionContext, sender: ActorRef = Actor.noSender): Cancellable = {
      val scheduler = context.system.scheduler
      val cancellable = scheduler.schedule(initialDelay, interval, receiver, message)(executor,self)
      context.system.log.debug("The message [{}] has been scheduled for send to [{}].", message, receiver.path)
      cancellable
    }

  def scheduleTaskOnce(delay: FiniteDuration, receiver: ActorRef, message: Any)
                      (implicit context: ActorContext, executor: ExecutionContext, sender: ActorRef = Actor.noSender): Cancellable = {
      val scheduler = context.system.scheduler
      val cancellable = scheduler.scheduleOnce(delay, receiver, message)(executor,self)
      context.system.log.debug("The message [{}] has been scheduled for send to [{}].", message, receiver.path)
      cancellable
    }

  def cancelTask(key: String, task: Cancellable)(implicit context: ActorContext): Boolean = {
    task.cancel()
    val isCancelled = task.isCancelled
    if (isCancelled)
      context.system.log.debug("The task [{}] has been cancelled.", key)
    else
      context.system.log.debug("Failed to cancel the task [{}].", key)
    isCancelled
  }
}
