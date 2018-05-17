package com.raphtory.core.clustersetup

/**
  * Created by Mirate on 13/06/2017.
  */
import akka.actor.Props

import scala.language.postfixOps
import scala.sys.process._

case class UpdateNode(seedLoc: String, className: String) extends DocSvr {
  implicit val system = init(List(seedLoc))
  //"redis-server /etc/redis/redis.conf --daemonize yes" ! ; //start redis running on manager partition
  system.actorOf(Props(Class.forName(className)), "UpdateGen")
}
