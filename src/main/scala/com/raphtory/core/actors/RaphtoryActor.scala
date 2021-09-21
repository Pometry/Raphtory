package com.raphtory.core.actors

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Cancellable, Timers}
import com.raphtory.core.actors.RaphtoryActor.{partitionServers, partitionsPerServer, builderServers, totalPartitions, totalBuilders, buildersPerServer}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

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

  def getWriter(srcId: Long): String = {
     s"/user/write_${(srcId.abs % totalPartitions).toInt }"
  }

  def getAllGraphBuilders(): Array[String] = {
    val workers = mutable.ArrayBuffer[String]()
    for (i <- 0 until totalBuilders)
      workers += s"/user/route_$i"
    workers.toArray
  }


  def getAllPartitionManagers(): Array[String] = {
    val workers = mutable.ArrayBuffer[String]()
    for (i <- 0 until partitionServers)
      workers += s"/user/Manager_$i"
    workers.toArray
  }

  def getAllReaders(): Array[String] = {
    val workers = mutable.ArrayBuffer[String]()
    for (i <- 0 until totalPartitions)
        workers += s"/user/read_$i"
    workers.toArray
  }

  def getAllWriters(): Array[String] = {
    val workers = mutable.ArrayBuffer[String]()
    for (i <- 0 until totalPartitions)
      workers += s"/user/write_$i"
    workers.toArray
  }


  def scheduleTask(initialDelay: FiniteDuration, interval: FiniteDuration, receiver: ActorRef, message: Any)(
    implicit context: ActorContext,
    executor: ExecutionContext,
    sender: ActorRef = Actor.noSender
  ): Cancellable = {
    val scheduler = context.system.scheduler

    val cancellable = scheduler.schedule(initialDelay, interval, receiver, message)(executor,self)
    context.system.log.debug("The message [{}] has been scheduled for send to [{}].", message, receiver.path)

    cancellable
  }

  def scheduleTaskOnce(
                        delay: FiniteDuration,
                        receiver: ActorRef,
                        message: Any
                      )(implicit context: ActorContext, executor: ExecutionContext, sender: ActorRef = Actor.noSender): Cancellable = {
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

  object sortOrdering extends Ordering[Long] {
    def compare(key1: Long, key2: Long) = key2.compareTo(key1)
  }

}
