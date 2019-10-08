package com.raphtory.examples.gab.actors

import java.util
import java.util.Properties

import com.raphtory.core.components.Spout.SpoutTrait
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}
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

    context.system.scheduler.scheduleOnce( Duration(10, SECONDS), self, "newLine")

  }

  protected def processChildMessages(message: Any): Unit = {
      message match {
        case "newLine" => {
          if (isSafe()) {
            //sendCommand(line)
            //println("hello")
            consumeFromKafka()
          }
          else {
            context.system.scheduler.scheduleOnce( Duration(1, MILLISECONDS), self, "newLine")
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
    context.system.scheduler.scheduleOnce( Duration(1, MILLISECONDS), self, "newLine")
  }
  def running(): Unit = {
    //genRandomCommands(totalCount)
    //totalCount+=1000
  }
}

