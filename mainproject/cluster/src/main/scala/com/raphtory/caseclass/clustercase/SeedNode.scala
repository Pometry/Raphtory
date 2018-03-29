package com.raphtory.caseclass.clustercase

import akka.actor.{Actor, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import com.raphtory.caseclass.DocSvr
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.concurrent.TrieMap

case class SeedNode(seedLoc:String) extends DocSvr {
  val conf : Config = ConfigFactory.load()
  implicit val system = init(List(seedLoc))
  system.actorOf(Props(new ClusterActor(this)), "cluster")
}

class ClusterActor(svr:DocSvr) extends Actor {
  val cluster = Cluster(context.system)

  val clusterMap : TrieMap[Set[String], Int] = TrieMap()

  // subscribe to cluster events
  override def preStart() : Unit = {
    println("== Starting cluster listener 2 ==")
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop() : Unit = cluster.unsubscribe(self)

  def receive = {

    // cluster event sent when a new cluster member comes up. Register the new cluster member if it is the parent node
    case state: MemberUp => {
      println("--1-- " + state.member)
      /*svr.nodes.synchronized {
        svr.nodes += state.member
        clusterMap.putIfAbsent(state.member.roles, 0) match {
          case Some(v) => {
            println(s"Not the first with role ${state.member.roles}")
            clusterMap.update(state.member.roles, v + 1)
            println(clusterMap)
          }
          case None    => {
            println(s"First ${state.member.roles} Up")
            clusterMap.update(state.member.roles, 1)
          }
        }

        // TODO Send broadcast new number of replicas (ie. PartitionManager, Routers)
        // TODO Send to new member its replica Id
      }*/
    }

    // cluster event sent when a cluster member is removed. Unregister the cluster member if it is the parent node
    case state: MemberRemoved => {
      println("--2--")
      svr.nodes.synchronized {
        svr.nodes -= state.member
      }
    }

    case state: UnreachableMember => {
      println("--3--")
      svr.nodes.synchronized {
        svr.nodes -= state.member
      }
    }

    case state: MemberExited => {
      println("--4--")
      svr.nodes.synchronized {
        svr.nodes -= state.member
      }
    }

    case _ => //println(s"Hmm... unknown event $z")
  }
}
