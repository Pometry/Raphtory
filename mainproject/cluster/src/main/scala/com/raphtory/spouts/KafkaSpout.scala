package com.raphtory.spouts

import java.util
import java.util.Properties

import com.raphtory.core.components.Spout.SpoutTrait
import com.raphtory.core.components.Spout.SpoutTrait.CommonMessage.Next
import com.raphtory.core.components.Spout.SpoutTrait.{BasicDomain, DomainMessage}
import com.raphtory.core.model.communication.StringSpoutGoing
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Random

final case class KafkaSpout() extends SpoutTrait[BasicDomain, StringSpoutGoing] {
  log.info("initialising KafkaSpout")
  private val kafkaServer   = System.getenv().getOrDefault("KAFKA_ADDRESS", "127.0.0.1").trim
  private val kafkaIp       = System.getenv().getOrDefault("KAFKA_PORT", "9092").trim
  private val offset        = System.getenv().getOrDefault("KAFKA_OFFSET", "earliest").trim
  private val groupId       = System.getenv().getOrDefault("KAFKA_GROUP", "group" + Random.nextLong()).trim
  private val topic         = System.getenv().getOrDefault("KAFKA_TOPIC", "sample_topic").trim
  private val restart       = System.getenv().getOrDefault("RESTART_RATE", "10").trim.toInt
  private val startingSpeed = System.getenv().getOrDefault("STARTING_SPEED", "1000").trim.toInt

  private var kafkaManager = KafkaManager(kafkaServer, kafkaIp, groupId, topic, offset)

  override def startSpout(): Unit =
    self ! Next

  def handleDomainMessage(message: BasicDomain): Unit = message match {
    case Next =>
      val (newManager, block) = kafkaManager.nextNLine(startingSpeed / 100)
      kafkaManager = newManager
      block.foreach(str => sendTuple(StringSpoutGoing(str)))
      context.system.scheduler.scheduleOnce(restart.millis, self, Next)
  }
}


final case class KafkaManager private (buffer: Stream[String], consumer: KafkaConsumer[String, String]) {
  private def poll(): KafkaManager = {
    // this is blocking operation which may waster some resource.
    // But it should only block when no data which may still make sense.
    val incoming = consumer.poll(java.time.Duration.ofMillis(3000)).asScala.toList.map(_.value())
    this.copy(buffer = buffer ++ incoming)
  }

  @tailrec
  def nextNLine(blockSize: Int): (KafkaManager, List[String]) =
    if (buffer.isEmpty)
      poll().nextNLine(blockSize)
    else {
      val (take, rest) = buffer.splitAt(blockSize)
      (this.copy(buffer = rest), take.toList)
    }
}

object KafkaManager {
  def apply(server: String, ip: String, groupId: String, topic: String, offset: String): KafkaManager = {
    val props = new Properties()
    props.put("bootstrap.servers", s"$server:$ip")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", offset)
    props.put("group.id", groupId)
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(topic))
    KafkaManager(Stream.empty, consumer)
  }
}
