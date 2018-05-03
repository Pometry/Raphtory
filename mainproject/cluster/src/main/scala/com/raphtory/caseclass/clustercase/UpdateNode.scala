package com.raphtory.caseclass.clustercase

/**
  * Created by Mirate on 13/06/2017.
  */
import akka.actor.Props
import com.raphtory.Actors.RaphtoryActors.DataSource.{GabSpout, UpdateGen, bitcoinSpout}
import com.raphtory.caseclass.DocSvr
import scala.sys.process._
import scala.language.postfixOps

case class UpdateNode(seedLoc: String, className: String) extends DocSvr {
  implicit val system = init(List(seedLoc))
  "redis-server /etc/redis/redis.conf --daemonize yes" ! ; //start redis running on manager partition
  system.actorOf(Props(Class.forName(className)), "UpdateGen")
}
