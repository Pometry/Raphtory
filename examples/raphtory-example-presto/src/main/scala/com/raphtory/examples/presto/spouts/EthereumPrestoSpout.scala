//TODO Presto spout needs to be updated to new API
//package com.raphtory.examples.presto.spouts
//
//import com.raphtory.examples.presto.EthereumTransaction
//import com.typesafe.config.{Config, ConfigFactory}
//import org.apache.pulsar.client.api.{Schema, SubscriptionInitialPosition, SubscriptionType}
//
//
//class EthereumPrestoSpout[T](schema: Schema[EthereumTransaction], resource: String, conf: Config = ConfigFactory.load) extends ResourceSpout[T](resource: String,  conf: Config) {
//
//  private val topic: String = conf.getString("Raphtory.spoutTopic")
//
//  private val consumer = createConsumer(topic)
//  val producer = createProducerx("eth_data", schema)
//
//  private def createConsumer(topic: String) = { //(implicit ev: Schema[T]) = {
//    println("Creating consumer")
//    PulsarController.accessClient.newConsumer()
//      .topic("raphtory_data_raw")
//      .subscriptionType(SubscriptionType.Exclusive)
//      .subscriptionName("spout_inter")
//      .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
//      .subscribe()
//  }
//
//  def createProducerx[T](topic: String, schema: Schema[T]) = { // (implicit ev: Schema[T]) = {
//    println("created producer")
//    PulsarController.accessClient.newProducer(schema)
//      .topic(topic)
//      .create()
//  }
//
//  def processMessage() = {
//    val msg = consumer.receive()
//    val line = new String(msg.getData)
//    val fileLine = line.replace("\"", "").split(",").map(_.trim)
//    if (fileLine(1) != "nonce") {
//      producer
//        .newMessage()
//        .key(fileLine(0))
//        .value(EthereumTransaction(
//          fileLine(0),
//          fileLine(1).toLong,
//          fileLine(2),
//          fileLine(3).toLong,
//          fileLine(4).toLong,
//          fileLine(5),
//          fileLine(6),
//          fileLine(7).toDouble,
//          fileLine(8).toLong,
//          fileLine(9).toLong,
//          fileLine(11).toLong,
//          fileLine(12),
//          fileLine(13),
//          fileLine(14).toLong)
//        )
//        .sendAsync()
//      consumer.acknowledge(msg.getMessageId)
//    }
//  }
//}
