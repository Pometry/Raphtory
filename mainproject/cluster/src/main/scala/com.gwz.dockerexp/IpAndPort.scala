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
  val INTERNAL_AKKA_PORT = 2551

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

class FailException(msg:String) extends Exception(msg)

case class DockerInspect (
                           Id    : String,
                           Ports : List[PortBinding]
                         )

case class PortBinding(
                        PrivatePort : Int,
                        PublicPort  : Option[Int]
                      )

/*  Docker remote call to <host>:5555/containers/json

Output:

[
  {
    "Command": "bin/root",
    "Created": 1432418467,
    "Id": "743a9a256262e427e8d630c785dc0f7d3016e06fed94baf6988691a89198c642",
    "Image": "quay.io/gzoller/root:latest",
    "Labels": {},
    "Names": [
      "/serene_leakey"
    ],
    "Ports": [
      {
        "IP": "0.0.0.0",
        "PrivatePort": 8080,
        "PublicPort": 8090,
        "Type": "tcp"
      },
      {
        "PrivatePort": 2551,
        "Type": "tcp"
      },
      {
        "IP": "0.0.0.0",
        "PrivatePort": 1600,
        "PublicPort": 2551,
        "Type": "tcp"
      }
    ],
    "Status": "Up Less than a second"
  }
]
*/


/*

AWS Notes:

1) Need to map /var/run/docker.sock to a http port by adding this to  /etc/sysconfig/docker
	OPTIONS="-H 0.0.0.0:5555 -H unix:///var/run/docker.sock"

	Note that this is insecure so [TODO] there is a way to secure this!  Google is your friend.

2) Run on AWS with: sudo docker run -it -p 9101:2551 -p 9100:8080 quay.io/gzoller/root
*/