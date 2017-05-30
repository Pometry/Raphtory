package com.gwz.dockerexp

import org.scalatest._
import org.scalatest.Matchers._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import com.typesafe.config.ConfigFactory
import akka.actor.{ActorSystem, ActorContext}
import java.net.InetAddress
import akka.pattern.ask
import akka.util.Timeout
import scala.language.postfixOps

import scala.sys.process._
import scala.util.Try

package object MyAddr {
	val myaddr = InetAddress.getLocalHost().getHostAddress()
}
import MyAddr._

class DockTests extends FunSpec with Matchers with BeforeAndAfterAll {

	implicit val t:Timeout = 15.seconds

	def get( uri:String ) = Try{scala.io.Source.fromURL(uri,"utf-8").getLines.fold("")( (a,b) => a + b )}.toOption

	override def beforeAll() {
	println("My IP: "+myaddr)
		val seedCmd = s"""docker run --name test_seed --rm -p 9101:2551 -e HOST_IP=$myaddr -e HOST_PORT=9101 dockerexp/cluster seed"""
		println("Seed: "+seedCmd)
		Future(seedCmd.!)
		Thread.sleep(7000)
		val restCmd = s"""docker run --name test_rest --rm -p 9102:2551 -p 8080:8080 -e HOST_IP=$myaddr -e HOST_PORT=9102 dockerexp/cluster rest $myaddr:9101"""
		println("Rest: "+restCmd)
		Future(restCmd.!)
		Thread.sleep(7000)
	}

	override def afterAll() {
		s"""docker kill test_seed""".!
		s"""docker kill test_rest""".!
		s"""docker kill test_n1""".!
		s"""docker kill test_n2""".!
	}

	describe("========= Test It!") {
		it("should ping") {
			val ping = get( s"""http://$myaddr:8080/ping""" )
			println(s"Getting http://$myaddr:8080/ping produced $ping")
			ping shouldBe defined
			ping.get should fullyMatch regex """\{"resp":".* says pong"\}"""
		}
		it("should know its other nodes") {
			Future( s"""docker run --name test_n1 --rm -p 9103:2551 -e HOST_IP=$myaddr -e HOST_PORT=9103 dockerexp/cluster logic $myaddr:9101""".! )
			Future( s"""docker run --name test_n2 --rm -p 9104:2551 -e HOST_IP=$myaddr -e HOST_PORT=9104 dockerexp/cluster logic $myaddr:9101""".! )
			Thread.sleep(7000)

			val nodes = get(s"""http://$myaddr:8080/nodes""")
			nodes shouldBe defined
			nodes.get should fullyMatch regex """\{"nodes":"\[.*\]\}"""
			nodes.get contains """:9101""" shouldBe true
			nodes.get contains """:9102""" shouldBe true
			nodes.get contains """:9103""" shouldBe true
			nodes.get contains """:9104""" shouldBe true
		}
		it("should akka") {
			val svc1 = get(s"""http://$myaddr:8080/svc""")
			svc1 shouldBe defined
			svc1.get should fullyMatch regex """\{"cluster_node":".* says 'you'"\}"""
			val svc2 = get(s"""http://$myaddr:8080/svc""")
			svc2 shouldBe defined
			svc2.get should fullyMatch regex """\{"cluster_node":".* says 'you'"\}"""
			val svc3 = get(s"""http://$myaddr:8080/svc""")
			svc3 shouldBe defined
			svc3.get should fullyMatch regex """\{"cluster_node":".* says 'you'"\}"""

			// Make sure some load balancing is going on
			val histogram = scala.collection.mutable.HashMap[String,Int]()  
			val items = List(svc1.get,svc2.get,svc3.get)
			items foreach { (x) => histogram += x -> (histogram.getOrElse(x, 0) + 1) }
			histogram.values should contain( 2 )
			histogram.values should contain( 1 )
		}
		it("kill one") {
			s"""docker kill test_n2""".!
			Thread.sleep(15000) // let failure propagate
			// Thread.sleep(10000) // let failure propagate
			// println("Killed a node!!! XXXXXX")
			// Thread.sleep(10000) // let failure 
			// println("Let's move on...")
			val svc1 = get(s"""http://$myaddr:8080/svc""")
			svc1 shouldBe defined
			svc1.get should fullyMatch regex """\{"cluster_node":".* says 'you'"\}"""
			val svc2 = get(s"""http://$myaddr:8080/svc""")
			svc2 shouldBe defined
			svc2.get should fullyMatch regex """\{"cluster_node":".* says 'you'"\}"""
			val svc3 = get(s"""http://$myaddr:8080/svc""")
			svc3 shouldBe defined
			svc3.get should fullyMatch regex """\{"cluster_node":".* says 'you'"\}"""

			// Make sure some load balancing is going on
			val histogram = scala.collection.mutable.HashMap[String,Int]()  
			val items = List(svc1.get,svc2.get,svc3.get)
			items foreach { (x) => histogram += x -> (histogram.getOrElse(x, 0) + 1) }
			histogram.values.size shouldBe 1
			histogram.values should contain( 3 )
		}
	}
}
