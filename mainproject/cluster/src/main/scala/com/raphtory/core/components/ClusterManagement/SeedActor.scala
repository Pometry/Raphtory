package com.raphtory.core.components.ClusterManagement

import akka.actor.Actor
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import com.raphtory.core.clustersetup.DocSvr

import scala.collection.concurrent.TrieMap

class SeedActor(svr:DocSvr) extends Actor {
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
      svr.nodes.synchronized {
        svr.nodes += state.member
      }
    }

    // cluster event sent when a cluster member is removed. Unregister the cluster member if it is the parent node
    case state: MemberRemoved => {
      println("--2--"+ state.member)
      svr.nodes.synchronized {
        svr.nodes -= state.member
      }
    }

    case state: UnreachableMember => {
      println("--3--" + state.member)
      //svr.nodes.synchronized {
      //  svr.nodes -= state.member
     // }
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