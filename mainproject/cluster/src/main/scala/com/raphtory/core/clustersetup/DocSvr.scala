package com.raphtory.core.clustersetup

import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.ExtendedActorSystem
import akka.cluster.Cluster
import akka.cluster.Member
import akka.event.LoggingAdapter
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.javadsl.AkkaManagement
import com.raphtory.core.clustersetup.util.ConfigUtils._
import com.raphtory.core.utils.Utils
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory

import scala.collection.JavaConversions
import scala.collection.JavaConversions._

trait DocSvr {

  def seedLoc: String

  implicit val system: ActorSystem
  val docker = System.getenv().getOrDefault("DOCKER", "false").trim.toBoolean

  val clusterSystemName: String = Utils.clusterSystemName
  val ssn: String               = java.util.UUID.randomUUID.toString

  /** Node lookup directory for registered members in the System */
  var nodes = Set.empty[Member]

  /** Initialise a new ActorSystem with configured name and seed nods
    *
    * @param seeds the set of Seed nodes to be added to the System
    * @return A new Akka ActorSystem object with the set config and seed nodes
    *         as determined by the ${seeds} parameter
    */
  def initialiseActorSystem(seeds: List[String]): ActorSystem = {
    var config = ConfigFactory.load()
    if (docker)
      config = config.withValue(
              "akka.cluster.seed-nodes",
              ConfigValueFactory.fromIterable(
                      JavaConversions.asJavaIterable(
                              seeds.map(_ => s"akka.tcp://$clusterSystemName@$seedLoc")
                      )
              )
      )

    val actorSystem = ActorSystem(clusterSystemName, config)
    if (!docker) {
      AkkaManagement.get(actorSystem).start()
      ClusterBootstrap.get(actorSystem).start()
    }
    printConfigInfo(config, actorSystem)
    actorSystem
  }

  /** Returns the set of Nodes in this ActorSystem
    *
    * @return A string object representing the address and roles of the members in this System
    */
  def getNodes: String =
    nodes.map(member => s"${member.address} ${member.getRoles.mkString}").mkString(",")

  /** Utility method to print the configuration which an ActorSystem has been created under
    *
    * @param config a TypeSafe config object detailing the Akka system configuration
    * @param system an initialised ActorSystem object
    */
  def printConfigInfo(config: Config, system: ActorSystem): Unit = {
    val log: LoggingAdapter = system.log

    val systemConfig: SystemConfig = config.parse()
    val bindAddress: SocketAddress = systemConfig.bindAddress
    val tcpAddress: SocketAddress  = systemConfig.tcpAddress

    log.info(s"Created ActorSystem with ID: $ssn")

    log.info(s"Binding ActorSystem internally to address ${bindAddress.host}:${bindAddress.port}")
    log.info(s"Binding ActorSystem externally to host ${tcpAddress.host}:${tcpAddress.port}")

    log.info(s"Registering the following seeds to ActorSystem: ${systemConfig.seeds}")
    log.info(s"Registering the following roles to ActorSystem: ${systemConfig.roles}")

    // FIXME: This is bit unorthodox ...
    val akkaSystemUrl: Address = system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
    log.info(s"ActorSystem successfully initialised at the following Akka URL: $akkaSystemUrl")
  }
}
