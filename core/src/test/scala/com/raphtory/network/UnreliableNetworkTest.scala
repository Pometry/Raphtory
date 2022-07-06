package com.raphtory.network

import cats.effect.IO
import cats.effect.Resource
import cats.effect.unsafe.implicits.global
import org.scalatest.funsuite.AnyFunSuite
import com.dimafeng.testcontainers.DockerComposeContainer
import com.dimafeng.testcontainers.ForAllTestContainer
import com.raphtory.Raphtory
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.communication.ExclusiveTopic
import com.raphtory.internals.communication.connectors.AkkaConnector
import munit.CatsEffectSuite
import org.scalatest.DoNotDiscover

import java.io.File
import scala.annotation.tailrec

sealed trait Request
case object IncrementCount extends Request
case object GetCount       extends Request

case class Result(value: Int)

@DoNotDiscover
class UnreliableNetworkTest extends AnyFunSuite with ForAllTestContainer {
  private val virtualPort = Raphtory.getDefaultConfig().getInt("raphtory.akka.port")
  private val actualPort  = virtualPort + 1
  private val env         = Map("INPUT_PORT" -> s"$virtualPort", "OUTPUT_PORT" -> s"$actualPort")
  private val config      = Raphtory.getDefaultConfig(Map("raphtory.akka.bindPort" -> s"$actualPort"))
  private val composeFile = new File("core/src/test/scala/com/raphtory/network/docker-compose.yml")

  override val container: DockerComposeContainer = DockerComposeContainer(composeFile, env = env)

//  private val listeningConnector: Fixture[AkkaConnector] =
//    ResourceSuiteLocalFixture("listening-connector", AkkaConnector[IO](AkkaConnector.SeedMode, config))
//
//  private val clientConnector: Fixture[AkkaConnector] =
//    ResourceSuiteLocalFixture("client-connector", AkkaConnector[IO](AkkaConnector.ClientMode, config))

  private val makeListeningConnector = AkkaConnector[IO](AkkaConnector.SeedMode, config)

  private val makeClientConnector = AkkaConnector[IO](AkkaConnector.ClientMode, config)

  private val connectors = for {
    listeningConnector <- makeListeningConnector
    clientConnector    <- makeClientConnector
  } yield (listeningConnector, clientConnector)

  @tailrec
  private def sendNMessages(endPoint: EndPoint[Request], numberOfMessages: Int): Unit =
    if (numberOfMessages > 0) {
      endPoint sendAsync IncrementCount
      sendNMessages(endPoint, numberOfMessages - 1)
    }

  @tailrec
  private def waitForExpectedValue(startingValue: Int, expectedCount: Int): Int = {
    Thread.sleep(1000)
    val currentCount   = Counter.count
    val reachedLimit   = currentCount >= expectedCount
    val makingProgress = currentCount > startingValue
    if (!reachedLimit && makingProgress)
      waitForExpectedValue(currentCount, expectedCount)
    else
      currentCount
  }

  test("package drop") {

    val messagesToSend = 1000

    val countResultComputation = connectors.use {
      case (listeningConnector, clientConnector) =>
        val listeningTopic = ExclusiveTopic[Request](listeningConnector, "request-topic")
        val listener       = listeningConnector.register("id", Counter.handleMessage, Seq(listeningTopic))
        listener.start()

        val endPoint = ExclusiveTopic[Request](clientConnector, "request-topic").endPoint

        sendNMessages(endPoint, messagesToSend)
        IO(waitForExpectedValue(0, messagesToSend))
    }

    val countResult = countResultComputation.unsafeRunSync()

    assert(countResult === messagesToSend)
  }
}

object Counter {
  var count = 0

  def handleMessage(msg: Request): Unit =
    msg match {
      case IncrementCount =>
        Counter.count += 1
      case GetCount       =>

    }
}
