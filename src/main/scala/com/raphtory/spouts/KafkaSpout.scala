package com.raphtory.spouts

import java.util
import java.util.Properties

import com.raphtory.core.components.spout.Spout
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

final case class KafkaSpout(IP:String,port:String,offset:String="earliest",groupId:String = "group" + Random.nextLong(), topic:String) extends Spout[String] {

  private var kafkaManager = KafkaManager(IP, port, groupId, topic, offset)
  val messageQueue = mutable.Queue[String]()

  override def setupDataSource(): Unit = {}

  override def generateData(): Option[String] = {
    if(messageQueue isEmpty) {
      val (newManager, block) = kafkaManager.nextNLine( 10)
      kafkaManager = newManager
      block.foreach(str => messageQueue += str)
      if(messageQueue isEmpty) //still empty
        None
    }
    Some(messageQueue.dequeue())
  }

  override def closeDataSource(): Unit = {}


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
