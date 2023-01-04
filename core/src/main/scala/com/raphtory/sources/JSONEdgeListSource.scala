package com.raphtory.sources

import com.raphtory.api.input.Graph.assignID
import com.raphtory.api.input.Graph
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Source
import com.raphtory.api.input.Spout
import com.raphtory.api.input.SpoutBuilderSource
import com.raphtory.api.input.Type
import com.raphtory.spouts.FileSpout
import com.raphtory.spouts.ResourceSpout

/**
  * JSONEdgeListSource is for building graphs in Raphtory from JSON data with no nested classes in JSON objects.
  *
  * @param spout : state where to ingest your data from (Mandatory field)
  * @param sourceKey : state the key of the source ID in your JSON data (default = "source")
  * @param sourceType : state what type of value the source is (default = None)
  * @param targetKey : state the key of the target ID in your JSON data (default = "target")
  * @param targetType : state what type of value the target is (default = None)
  * @param edgeRelationship : state what the edge relationship is (default = None)
  * @param timeKey : state the key of the timestamp in your JSON data, timestamp must be in datetime format (default = "time")
  */

class JSONEdgeListSource(
    override val spout: Spout[String],
    sourceKey: String = "source",
    sourceType: String = "",
    targetKey: String = "target",
    targetType: String = "",
    edgeRelationship: String = "",
    timeKey: String = "time"
) extends SpoutBuilderSource[String] {

  override def builder: GraphBuilder[String] =
    (graph: Graph, jsonString: String) => {
      val str       = jsonString.stripMargin
      val json      = ujson.read(str)
      val src       = json(sourceKey).toString()
      val dst       = json(targetKey).toString()
      val timestamp = graph.parseDatetime(json(timeKey).toString().replace("\"", ""))
      val srcID     = assignID(src)
      val dstID     = assignID(dst)

      if (src.nonEmpty)
        graph.addVertex(timestamp, srcID, Type(sourceType))

      if (dst.nonEmpty)
        graph.addVertex(timestamp, dstID, Type(targetType))

      if (src.nonEmpty && dst.nonEmpty)
        graph.addEdge(timestamp, srcID, dstID, Type(edgeRelationship))
    }

}

object JSONEdgeListSource {

  def apply(
      spout: Spout[String],
      sourceKey: String = "source",
      sourceType: String = "",
      targetKey: String = "target",
      targetType: String = "",
      edgeRelationship: String = "",
      timeKey: String = "time"
  ) = new JSONEdgeListSource(spout, sourceKey, sourceType, targetKey, targetType, edgeRelationship, timeKey)

  def fromFile(
      path: String,
      sourceKey: String = "source",
      sourceType: String = "",
      targetKey: String = "target",
      targetType: String = "",
      edgeRelationship: String = "",
      timeKey: String = "time"
  ) = new JSONEdgeListSource(FileSpout(path), sourceKey, sourceType, targetKey, targetType, edgeRelationship, timeKey)

  def fromResource(
      path: String,
      sourceKey: String = "source",
      sourceType: String = "",
      targetKey: String = "target",
      targetType: String = "",
      edgeRelationship: String = "",
      timeKey: String = "time"
  ) =
    new JSONEdgeListSource(ResourceSpout(path), sourceKey, sourceType, targetKey, targetType, edgeRelationship, timeKey)
}
