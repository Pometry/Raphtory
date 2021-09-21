package com.raphtory.core.components.orchestration.componentconnector


import akka.actor.{ActorRef, Props}
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.pattern.ask
import com.raphtory.core.components.RaphtoryActor.{partitionServers, partitionsPerServer}
import com.raphtory.core.components.orchestration.raphtoryleader.WatchDog.Message.RequestPartitionId
import com.raphtory.core.components.partitionmanager.{PartitionManager, Reader, Writer}
import com.raphtory.core.implementations.objectgraph.ObjectBasedPartition
import com.raphtory.core.model.graph.GraphPartition

import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.Future

class PartitionConnector() extends ComponentConnector() {
  override def callTheWatchDog(): Future[Any] = {
    log.debug(s"Attempting to retrieve Partition Id from WatchDog.")
    mediator ? DistributedPubSubMediator.Send("/user/WatchDog", RequestPartitionId, localAffinity = false)
  }

  override def giveBirth(assignedId: Int): Unit = {
    log.info(s"Partition Manager $assignedId has come online.")

    var writers:  ParTrieMap[Int, ActorRef]       = new ParTrieMap[Int, ActorRef]()
    var storages: ParTrieMap[Int, GraphPartition] = new ParTrieMap[Int, GraphPartition]()
    var readers:  ParTrieMap[Int, ActorRef]       = new ParTrieMap[Int, ActorRef]()
    val startRange = assignedId*partitionsPerServer
    val endRange = startRange+partitionsPerServer
    for (index <- startRange until endRange) {
      val storage     = new ObjectBasedPartition(index)
      storages.put(index, storage)

      val writeName = s"write_$index"
      val readerName = s"read_$index"
      writers.put(
        index,
        context.system
          .actorOf(Props(new Writer(index, storage)).withDispatcher("worker-dispatcher"), writeName)
      )
      readers.put(
        index,
        context.system
          .actorOf(Props(Reader(index, storage)).withDispatcher("reader-dispatcher"), readerName)
      )
    }


    actorRef = context.system.actorOf(Props(new PartitionManager(myId, writers,readers,storages)), s"Manager_$myId")

  }

}
