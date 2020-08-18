package com.raphtory.examples.gab.actors

import java.util
import java.util.Properties

import com.raphtory.core.components.Spout.SpoutTrait
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.duration.MILLISECONDS
import scala.concurrent.duration.SECONDS
import scala.util.Random
class GabKafkaSpout extends SpoutTrait {
  val x     = new Random().nextLong()
  val props = new Properties()
  props.put("bootstrap.servers", "moe.eecs.qmul.ac.uk:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("auto.offset.reset", "earliest")
  props.put("group.id", "group" + x)
  val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
  consumer.subscribe(util.Arrays.asList("gabResortedGraph"))

  protected def ProcessSpoutTask(message: Any): Unit = message match {
    case StartSpout => AllocateSpoutTask(Duration(1, MILLISECONDS), "newLine")
    case "newLine"  => consumeFromKafka()
    case _          => println("message not recognized!")
  }

  def consumeFromKafka() = {
    val record = consumer.poll(1000).asScala
    for (data <- record.iterator)
      sendTuple(data.value())
    AllocateSpoutTask(Duration(1, MILLISECONDS), "newLine")
  }
}
