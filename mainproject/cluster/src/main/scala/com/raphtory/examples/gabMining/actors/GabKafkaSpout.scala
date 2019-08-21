package com.raphtory.examples.gabMining.actors

import java.util

import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import java.util.Properties

import com.raphtory.core.components.Spout.SpoutTrait
import kafka.serializer.DefaultDecoder
import kafka.utils.Whitelist

import scala.collection.JavaConverters._
import scala.concurrent.duration.{Duration, NANOSECONDS, SECONDS}
import scala.util.Random
class GabKafkaSpout extends SpoutTrait{
  val x = new Random().nextLong()
  val props = new Properties()
  props.put("bootstrap.servers", "moe.eecs.qmul.ac.uk:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("auto.offset.reset", "earliest")
  props.put("group.id", "group"+x)
  val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
  consumer.subscribe(util.Arrays.asList("gabGraph"))

  override def preStart() { //set up partition to report how many messages it has processed in the last X seconds
    super.preStart()

    context.system.scheduler.schedule( Duration(10, SECONDS),Duration(100000, NANOSECONDS), self, "newLine")

  }

  protected def processChildMessages(message: Any): Unit = {
      message match {
        case "newLine" => {
          if (isSafe()) {
            //sendCommand(line)
            //println("hello")
            consumeFromKafka()
          }
        }
        case "stop" => stop()
        case _ => println("message not recognized!")
      }
  }


  def consumeFromKafka() = {

    //for(i <- 1 to 10) {
      val record = consumer.poll(1000).asScala
      for (data <- record.iterator)
        sendCommand(data.value())
    //}
  }
  def running(): Unit = {
    //genRandomCommands(totalCount)
    //totalCount+=1000
  }
}

