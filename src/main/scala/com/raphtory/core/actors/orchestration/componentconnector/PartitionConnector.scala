package com.raphtory.core.actors.orchestration.componentconnector


import akka.actor.{ActorRef, Props}
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.pattern.ask
import com.raphtory.core.actors.orchestration.clustermanager.WatchDog.Message.RequestPartitionId
import com.raphtory.core.actors.partitionmanager.workers.IngestionWorker
import com.raphtory.core.actors.partitionmanager.{Reader, Writer}
import com.raphtory.core.model.GraphPartition
import com.raphtory.core.model.implementations.objectgraph.ObjectBasedPartition

import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.Future

class PartitionConnector(managerCount: Int, routerCount:Int) extends ComponentConnector(initialManagerCount = managerCount,initialRouterCount = routerCount) {
  override def callTheWatchDog(): Future[Any] = {
    log.debug(s"Attempting to retrieve Partition Id from WatchDog.")
    mediator ? DistributedPubSubMediator.Send("/user/WatchDog", RequestPartitionId, localAffinity = false)
  }

  override def giveBirth(assignedId: Int): Unit = {
    log.info(s"Partition Manager $assignedId has come online.")

    var workers: ParTrieMap[Int, ActorRef]       = new ParTrieMap[Int, ActorRef]()
    var storages: ParTrieMap[Int, GraphPartition] = new ParTrieMap[Int, GraphPartition]()

    for (index <- 0 until totalWorkers) {
      val storage     = new ObjectBasedPartition(currentCount,assignedId,index)
      storages.put(index, storage)

      val managerName = s"Manager_${assignedId}_child_$index"
      workers.put(
        index,
        context.system
          .actorOf(Props(new IngestionWorker(index,assignedId, storage,currentCount)).withDispatcher("worker-dispatcher"), managerName)
      )
    }

    actorRef = context.system.actorOf(Props(new Writer(myId,  currentCount, workers, storages)), s"Manager_$myId")

    actorRefReader = context.system.actorOf(Props(new Reader(myId,  currentCount, storages)), s"ManagerReader_$myId")

  }

}
