//package com.raphtory.serialisers
//
//import com.raphtory.api.Serialiser
//import com.raphtory.core.model.analysis.entityVisitors.{EdgeVisitor, VertexVisitor}
//
//class CSVSerialiser extends Serialiser{
//  override def serialiseVertex(v: VertexVisitor): String = {
//    v.getHistory().filter(_._2).map{h=>
//      s"${h._1+1e9}, ${v.ID()}, ${v.getPropertyValue("commLabel").get}"
//    }.mkString("\n")
//  }
//
//  override def serialiseEdge(e: EdgeVisitor): String = ""
//
//  override def startOfFile(): String = ""
//
//  override def middleOfFile(): String = ""
//
//  override def endOfFile(): String = ""
//}
