package com.raphtory.api.input.sources

import com.jayway.jsonpath.JsonPath
import com.raphtory.api.input.Graph.assignID
import com.raphtory.api.input._
import com.raphtory.spouts.{FileSpout, ResourceSpout}

/**
 * * Specific format: NetworkX Node Link Data Format
 * https://networkx.org/documentation/stable/reference/readwrite/generated/networkx.readwrite.json_graph.node_link_data.html
 *
 * This is a generic source to ingest data and build a graph in Raphtory, assuming the data is in Network X JSON node link format,
 *
 * You can configure source and target type and edge relationship type when calling this source in your Runner.
 * You can also configure the key of your time value and edge type value if that exists in your data.
 * Properties and keys of properties in your NetworkX graph will need to be added manually.
 *
 * @param spout : state where to ingest your data from (Mandatory field)
 * @param sourceKey : state the key of the source ID in your JSON data (default = "source")
 * @param sourceType : state what type of value the source is (default = "Name")
 * @param targetKey : state the key of the target ID in your JSON data (default = "target")
 * @param targetType : state what type of value the target is (default = "Name")
 * @param edgeKey : state the key of the edge type/relationship in your JSON data (default = "Person to Person")
 * @param timeKey : state the key of the timestamp in your JSON data (default = 1)
 */

class JSONSource(override val spout: Spout[String], sourceKey: String = "source", sourceType: String = "Name", targetKey: String = "target", targetType: String = "Name", edgeKey: Option[String] = None, timeKey: Option[String] = None) extends Source {
  override type MessageType = String

  private var source: String = _
  private var target: String = _
  private var _type: Option[String] = None
  private var time: Option[Long] = None

  override def builder: GraphBuilder[String] =
    (graph: Graph, jsonString: String) => {
      val networkXGraph = ujson.Value(jsonString.stripMargin).obj.values.toSeq

      // Links is always fourth index of Network X graph
      networkXGraph(4).arr.map { json =>
        source = JsonPath.read[Any](json.toString(), "$." + sourceKey).toString
        target = JsonPath.read[Any](json.toString(), "$." + targetKey).toString
        if (timeKey.nonEmpty) {
          time = JsonPath.read[Any](json.toString(), "$." + timeKey).toString.toLongOption
        }
        if (edgeKey.nonEmpty) {
          _type = Option(JsonPath.read[Any](json.toString(), "$." + edgeKey).toString)
        }

        val srcID = assignID(source)
        val dstID = assignID(target)

        if (source.nonEmpty) {
          graph.addVertex(time.getOrElse(1), srcID, Type(sourceType))
        }

        if (target.nonEmpty) {
          graph.addVertex(time.getOrElse(1), dstID,Type(targetType))
        }

        if(source.nonEmpty && target.nonEmpty)
          graph.addEdge(time.getOrElse(1), srcID, dstID, Type(_type.getOrElse("Person to Person")))
      }
    }
}
object JSONSource {
  def apply(spout: Spout[String], sourceKey: String = "source", sourceType: String = "Name", targetKey: String = "target", targetType: String = "Name", edgeKey: Option[String] = None, timeKey: Option[String] = None) = new JSONSource(spout, sourceKey, sourceType, targetKey, targetType, edgeKey, timeKey)
  def fromFile(path: String,  sourceKey: String = "source", sourceType: String = "Name", targetKey: String = "target", targetType: String = "Name", edgeKey: Option[String] = None, timeKey: Option[String] = None) = new JSONSource(FileSpout(path),sourceKey, sourceType, targetKey, targetType, edgeKey, timeKey)
  def fromResource(path: String,sourceKey: String = "source", sourceType: String = "Name", targetKey: String = "target", targetType: String = "Name", edgeKey: Option[String] = None, timeKey: Option[String] = None) = new JSONSource(ResourceSpout(path), sourceKey, sourceType, targetKey, targetType, edgeKey, timeKey)
}