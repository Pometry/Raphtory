`com.raphtory.algorithms.generic.EdgeList`
(com.raphtory.algorithms.generic.EdgeList)=
# EdgeList

 {s}`EdgeList(properties: String*)`
 : Writes out edge list with selected properties to table with format srcName, dstName, edgeProperty1, ...

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

edge list with selected properties

 | source name          | destination name     | property 1      | ... |
 | -------------------- | -------------------- | --------------- | --- |
 | {s}`srcName: String` | {s}`dstName: String` | {s}`value: Any` | ... |