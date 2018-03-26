package com.gwz.dockerexp



object IpPort {

  val ENV_HOST_IP        = "HOST_IP"
  val ENV_HOST_PORT      = "HOST_PORT"
  val ENV_HOSTNAME       = "HOSTNAME"
  val ENV_EXT_AKKA       = "EXT_AKKA"
}
import IpPort._

case class IpAndPort() {
  val baseAdaptor = {
    val base = new EnvAdaptor(){}
      base
  }
  val hostIP   = baseAdaptor.hostIP.getOrElse( throw new FailException("Can't determine host IP") )
  val akkaPort = baseAdaptor.akkaPort.getOrElse(throw new FailException("Can't determine akka port"))
}

trait EnvAdaptor {
  val hostIP   : Option[String] = Option(System.getenv().get(ENV_HOST_IP))
  val akkaPort : Option[Int]    = Option(System.getenv().get(ENV_HOST_PORT)).map(_.toInt)
  def +( ea:EnvAdaptor ) : EnvAdaptor  = {
    val left = this
    new EnvAdaptor(){
      override val hostIP   : Option[String] = left.hostIP.orElse(ea.hostIP)
      override val akkaPort : Option[Int]    = left.akkaPort.orElse(ea.akkaPort)
    }
  }
}


class FailException(msg:String) extends Exception(msg)

case class DockerInspect (
                           Id    : String,
                           Ports : List[PortBinding]
                         )

case class PortBinding(
                        PrivatePort : Int,
                        PublicPort  : Option[Int]
                      )
