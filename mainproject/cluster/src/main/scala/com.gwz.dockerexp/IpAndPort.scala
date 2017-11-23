package com.gwz.dockerexp

import java.net._
import java.io._
import co.blocke.scalajack._
import scala.util.Try

/**
Figure out IP and port mappings for this running Docker depending on the
	deployment scenarios supported in the chart below:

		Run Method			Run Env		Host IP 		Port Mappings
		-------------------------------------------------------------------
		docker run 			non-EC2		passed-in		passed-in
		docker run 			non-EC2		passed-in		inspect agent

		docker run 			EC2			passed-in		passed-in
		docker run			EC2			passed-in		inspect agent

		ECS ("ocean")		EC2			metadata svc	inspect agent
  */

object IpPort {
  val AWS_LOCAL_HOST_IP  = "http://169.254.169.254/latest/meta-data/local-ipv4"
  val AWS_PUBLIC_HOST_IP = "http://169.254.169.254/latest/meta-data/public-ipv4"
  val INTERNAL_AKKA_PORT = 2552

  val ENV_HOST_IP        = "HOST_IP"
  val ENV_HOST_PORT      = "HOST_PORT"
  val ENV_HOSTNAME       = "HOSTNAME"
  val ENV_EXT_AKKA       = "EXT_AKKA"
}
import IpPort._

case class IpAndPort() {
  val baseAdaptor = {
    val base = new EnvAdaptor(){}
    if( base.hostIP.isEmpty || base.akkaPort.isEmpty )
      base + AwsEnvAdaptor()
    else
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
















// AWS EC2-specific Introspection
case class AwsEnvAdaptor() extends EnvAdaptor {
  private val sj = ScalaJack()

  def httpGetLite( uri:String ) = Try{scala.io.Source.fromURL(uri,"utf-8").getLines.fold("")( (a,b) => a + b )}.toOption

  override val hostIP   : Option[String] = {
    if( System.getenv().get(ENV_EXT_AKKA) == "true" )
      httpGetLite(AWS_PUBLIC_HOST_IP)  // Akka callable only outside AWS
    else
      httpGetLite(AWS_LOCAL_HOST_IP)   // Akka callable only inside AWS
  }
  override val akkaPort : Option[Int] = httpGetLite(AWS_LOCAL_HOST_IP).flatMap(local => inspectPort(local))

  def inspectPort(hIp:String) : Option[Int] = {
    val instId = System.getenv().get(ENV_HOSTNAME)
    httpGetLite(s"http://$hIp:5555/containers/json").flatMap( ins => {
      Try {
        val ob = sj.read[List[DockerInspect]](ins)
        ob.find( _.Id.startsWith(instId) ).map(_.Ports.find( _.PrivatePort == INTERNAL_AKKA_PORT ).map( _.PublicPort.get ).get ).get
      }.toOption
    })
  }
}