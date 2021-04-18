package com.raphtory.serialisers

import java.net.InetAddress

import com.mongodb.DBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.mongodb.util.JSON
import com.raphtory.core.analysis.api.AggregateSerialiser
import java.net.InetAddress
import scala.collection.JavaConverters._

object MongoSerialiser {
  new MongoSerialiser
}

class MongoSerialiser extends AggregateSerialiser{


  private val saveData  = System.getenv().getOrDefault("ANALYSIS_SAVE_OUTPUT", "false").trim.toBoolean
  private val mongoIP   = System.getenv().getOrDefault("ANALYSIS_MONGO_HOST", "localhost").trim
  private val mongoPort = System.getenv().getOrDefault("ANALYSIS_MONGO_PORT", "27017").trim
  private val dbname    = System.getenv().getOrDefault("ANALYSIS_MONGO_DB_NAME", "raphtory").trim
  private val mongo = MongoClient(MongoClientURI(s"mongodb://${InetAddress.getByName(mongoIP).getHostAddress}:$mongoPort"))

  override def serialiseView(results: Map[String, Any], timestamp: Long, jobID: String, viewTime: Long): Unit = {
    val data = results.values.map(x=> JSON.parse(x.toString).asInstanceOf[DBObject])
    if (data.nonEmpty) mongo.getDB(dbname).getCollection(jobID).insert(data.toList.asJava)
  }

  override def serialiseWindowedView(results: Map[String, Any], timestamp: Long, window: Long, jobID: String, viewTime: Long): Unit = ???
}
