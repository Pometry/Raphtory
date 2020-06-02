package com.raphtory.spouts

import java.util
import java.util.Properties

import com.raphtory.core.components.Spout.SpoutTrait
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.duration.MILLISECONDS
import scala.concurrent.duration.SECONDS
import scala.util.Random
class KafkaSpout extends SpoutTrait {
  var kafkaServer     = System.getenv().getOrDefault("KAFKA_ADDRESS", "127.0.0.1").trim
  var kafkaIP     = System.getenv().getOrDefault("KAFKA_PORT", "9092").trim
  var offset     = System.getenv().getOrDefault("KAFKA_OFFSET", "earliest").trim
  val x     = new Random().nextLong()
  var groupID   = System.getenv().getOrDefault("KAFKA_GROUP", "group" + x).trim
  var topic   = System.getenv().getOrDefault("KAFKA_TOPIC", "sample_topic").trim
  var restart   = System.getenv().getOrDefault("RESTART_RATE", "1000").trim

  val props = new Properties()
  props.put("bootstrap.servers", s"$kafkaServer:$kafkaIP")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("auto.offset.reset", offset)
  props.put("group.id", groupID)
  val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
  consumer.subscribe(util.Arrays.asList(topic))

  protected def ProcessSpoutTask(message: Any): Unit = message match {
    case StartSpout => AllocateSpoutTask(Duration(1, MILLISECONDS), "newLine")
    case "newLine"  => consumeFromKafka()
    case _          => println("message not recognized!")
  }

  def consumeFromKafka() = {
    val record = consumer.poll(java.time.Duration.ofMillis(1000)).asScala
    for (data <- record.iterator)
      //println(data.value())
      sendTuple(data.value())
    AllocateSpoutTask(Duration(restart.toInt, MILLISECONDS), "newLine")
  }
}
