package com.raphtory

object IpPort {

  val ENV_HOST_IP        = "HOST_IP"
  val ENV_HOST_PORT      = "HOST_PORT"
  val ENV_HOSTNAME       = "HOSTNAME"
  val ENV_EXT_AKKA       = "EXT_AKKA"
}
import IpPort._
import com.typesafe.config.ConfigFactory

class FailException(msg:String) extends Exception(msg)

case class DockerInspect (
                           Id    : String,
                           Ports : List[PortBinding]
                         )

case class PortBinding(
                        PrivatePort : Int,
                        PublicPort  : Option[Int]
                      )
