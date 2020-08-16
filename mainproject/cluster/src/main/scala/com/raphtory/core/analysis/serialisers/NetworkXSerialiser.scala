package com.raphtory.core.analysis.serialisers
import com.raphtory.core.analysis.API.entityVisitors.{EdgeVisitor, VertexVisitor}

class NetworkXSerialiser extends Serialiser {

  override def startOfFile(): String = "G = nx.Graph()"

  override def serialiseVertex(v: VertexVisitor): String = {
    val properties =v.getPropertySet().map(property => s"${property._1} = ${property._2}").toArray.mkString(",")
    if(properties.nonEmpty) {
      if (v.Type().nonEmpty)
        s"G.add_node(${v.ID()},type = ${v.Type()},$properties)"
      else
        s"G.add_node(${v.ID()},$properties)"
    }
    else {
      if (v.Type().nonEmpty)
        s"G.add_node(${v.ID()},type = ${v.Type()})"
      else
        s"G.add_node(${v.ID()})"
    }
  }

  override def serialiseEdge(e: EdgeVisitor): String = {
    val properties =e.getPropertySet().map(property => s"${property._1} = ${property._2}").toArray.mkString(",")
    if(properties.nonEmpty) {
      if (e.Type().nonEmpty)
        s"G.add_edge(${e.src()},${e.dst()},type = ${e.Type()},$properties)"
      else
        s"G.add_edge(${e.src()},${e.dst()},$properties)"
    }
    else {
      if (e.Type().nonEmpty)
        s"G.add_edge(${e.src()},${e.dst()},type = ${e.Type()})"
      else
        s"G.add_edge(${e.src()},${e.dst()})"
    }

  }

  override def endOfFile(): String = ""

}
