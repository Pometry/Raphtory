package com.raphtory.core.actors

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Cancellable, Timers}

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

trait RaphtoryActor extends Actor with ActorLogging with Timers {

  val partitionsTopic    = "/partitionsCount"
  val totalWorkers = 10 //must be power of 10

  //get the partition a vertex is stored in
  def checkDst(dstID: Long, managerCount: Int, managerID: Int): Boolean = ((dstID.abs % (managerCount * totalWorkers)) / totalWorkers).toInt == managerID //check if destination is also local
  def checkWorker(dstID: Long, managerCount: Int, workerID: Int): Boolean = ((dstID.abs % (managerCount * totalWorkers)) % totalWorkers).toInt == workerID //check if destination is also local

    def getManager(srcId: Long, managerCount: Int): String = {
      val mod     = srcId.abs % (managerCount * totalWorkers)
      val manager = mod / totalWorkers
      val worker  = mod % totalWorkers
      s"/user/Manager_${manager}_child_$worker"
    }

    def getReader(srcId: Long, managerCount: Int): String = {
      val mod     = srcId.abs % (managerCount * totalWorkers)
    val manager = mod / totalWorkers
    val worker  = mod % totalWorkers
    s"/user/Manager_${manager}_reader_$worker"
  }

  def getAllRouterWorkers(managerCount: Int): Array[String] = {
    val workers = mutable.ArrayBuffer[String]()
    for (i <- 0 until managerCount)
      for (j <- 0 until totalWorkers)
        workers += s"/user/router/router_${i}_Worker_$j"
    workers.toArray
  }


  def getAllReaders(managerCount: Int): Array[String] = {
    val workers = mutable.ArrayBuffer[String]()
    for (i <- 0 until managerCount)
      workers += s"/user/ManagerReader_$i"
    workers.toArray
  }

  def getAllReaderWorkers(managerCount: Int): Array[String] = {
    val workers = mutable.ArrayBuffer[String]()
    for (i <- 0 until managerCount)
      for (j <- 0 until totalWorkers)
        workers += s"/user/Manager_${i}_reader_$j"
    workers.toArray
  }

  def getAllJobWorkers(managerCount: Int,jobID:String): Array[String] = {
    val workers = mutable.ArrayBuffer[String]()
    for (i <- 0 until managerCount)
      for (j <- 0 until totalWorkers)
        workers += s"/user/Manager_${i}_reader_${j}_analysis_subtask_worker_$jobID"
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
