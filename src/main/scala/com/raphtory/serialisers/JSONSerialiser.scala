package com.raphtory.serialisers

import com.raphtory.core.analysis.api.Serialiser
import com.raphtory.core.analysis.entityVisitors.{EdgeVisitor, VertexVisitor}

import scala.collection.parallel.mutable.ParTrieMap

class JSONSerialiser extends Serialiser {

  override def startOfFile(): String = "{\"directed\": true, \"multigraph\": false, \"graph\": {}, \"nodes\": [\n"

  override def middleOfFile() :String = "],\n\"links\":[\n"

  override def fileExtension(): String = {
    "json"
  }

  override def serialiseVertex(v: VertexVisitor): String = {
    val properties: String = extractProperties(v.getPropertySet())

    if(properties.nonEmpty) {
      if (v.Type().nonEmpty)
        s"""\t{\"id\":${v.ID()},\"doctype\":\"${v.Type()}\",$properties}"""
      else
        s"""\t{\"id\":${v.ID()},$properties}"""
    }
    else {
      if (v.Type().nonEmpty)
        s"""\t{\"id\":${v.ID()},\"doctype\":\"${v.Type()}\"}"""
      else
        s"""\t{\"id\":${v.ID()}}"""
    }
  }

  private def extractProperties(ps: ParTrieMap[String,Any]) = {
    ps.map(property =>
      if (property._2.isInstanceOf[Long])
        s"""\"${property._1}\":${property._2}"""
      else
        s"""\"${property._1}\":\"${property._2}\""""
    ).toArray.mkString(",")
  }

  override def serialiseEdge(e: EdgeVisitor): String = {
    val properties = extractProperties(e.getPropertySet())
    if(properties.nonEmpty) {
      if (e.Type().nonEmpty)
        s"""\t{\"source\":${e.src()},\"target\":${e.dst()},\"edgetype\":\"${e.Type()}\",$properties}"""
      else
        s"""\t{\"source\":${e.src()},\"target\":${e.dst()},$properties}"""
    }
    else {
      if (e.Type().nonEmpty)
        s"""\t{\"source\":${e.src()},\"target\":${e.dst()},\"edgetype\":\"${e.Type()}\"}"""
      else
        s"""\t{\"source\":${e.src()},\"target\":${e.dst()}}"""
    }
  }

  override def endOfFile(): String = "\n]}\n"

}
