package com.gwz.dockerexp.Actors.ClusterActors

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._

class ClusterActor(svr:DocSvr) extends Actor {
	val cluster = Cluster(context.system)

	// subscribe to cluster events
	override def preStart():Unit = {
		println("== Starting cluster listener 2 ==")
		cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
	}

	override def postStop():Unit = cluster.unsubscribe(self)

	def receive = {

		// cluster event sent when a new cluster member comes up. Register the new cluster member if it is the parent node
		case state   : MemberUp       => {
			println("--1-- "+state.member)
			svr.nodes.synchronized{ svr.nodes += state.member }
		}

		// cluster event sent when a cluster member is removed. Unregister the cluster member if it is the parent node
		case state   : MemberRemoved  => {
			println("--2--")
			svr.nodes.synchronized{ svr.nodes -= state.member }
		}

		case state   : UnreachableMember  => {
			println("--3--")
			svr.nodes.synchronized{ svr.nodes -= state.member }
		}

		case state   : MemberExited  => {
			println("--4--")
			svr.nodes.synchronized{ svr.nodes -= state.member }
		}

		case z => println(s"Hmm... unknown event $z")
	}	
}