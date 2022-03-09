`com.raphtory.algorithms.temporal.TemporalEdgeList`
(com.raphtory.algorithms.temporal.TemporalEdgeList)=
# TemporalEdgeList

 {s}`TemporalEdgeList(properties: String*)`
 : Writes out temporal edge list with selected properties

 {s}`EdgeList(defaults: Map[String, Any], properties: String*)`
 : Specify default values for missing properties

 {s}`EdgeList(properties: Seq[String] = Seq.empty[String], defaults: Map[String, Any] = Map.empty[String, Any])`
 : Specify sequence of property names

 ## Parameters

 {s}`properties: Seq[String]`
 : Sequence of property names to extract (default: empty)

 {s}`defaults: Map[String, Any]`
 : Map of property names to default values (default value: None)

 ## Returns

temporal edge list with selected properties

 | source name          | destination name     | time stamp      |  property 1     | ... |
 | -------------------- | -------------------- | --------------- | --- ----------- | --- |
 | {s}`srcName: String` | {s}`dstName: String` | {s}`time: Long` | {s}`value: Any` | ... |