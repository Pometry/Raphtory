package com.raphtory.tests
import scala.collection.immutable.IndexedSeq

import org.mongodb.scala._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.model._

object MongoTest extends App {
  val client:MongoClient=MongoClient("mongodb://138.37.32.68:27017")
  val database: MongoDatabase = client.getDatabase("gab")
  val collection: MongoCollection[Document] = database.getCollection("posts")
}
