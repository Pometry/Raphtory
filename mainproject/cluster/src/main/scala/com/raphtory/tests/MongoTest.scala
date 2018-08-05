package com.raphtory.tests

import com.mongodb.casbah.Imports._
object MongoTest extends App {
  val mongoConn = MongoConnection("138.37.32.68", 27017)
  val mongoColl = mongoConn("gab")("posts")
  val q = MongoDBObject("_id" -> 2)
  //println( mongoColl.findOne(q))
  mongoColl.findOne(q) match {
    case e:Some[DBObject] => println(e.get("data").getClass)
    case None => {/*do nothing*/}
  }
}

