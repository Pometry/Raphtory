package com.gwz.dockerexp.Actors.ClusterActors

import akka.actor._
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}

class LogicActor(svr:DocSvr) extends Actor {

	val mediator = DistributedPubSub(context.system).mediator
	// subscribe to the topic named "logic"
	mediator ! DistributedPubSubMediator.Put(self)
	def receive = {
		case "hey" => 
			println("Received hey message...")
			sender ! svr.ssn+" says 'you'"
	}
}