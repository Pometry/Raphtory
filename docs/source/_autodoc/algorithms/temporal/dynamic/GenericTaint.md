`com.raphtory.algorithms.temporal.dynamic.GenericTaint`
(com.raphtory.algorithms.temporal.dynamic.GenericTaint)=
# GenericTaint

{s}`GenericTaint((startTime: Int, infectedNodes: Iterable[Long], stopNodes: Set[Long] = Set())`
 : temporal tainting/infection dynamic

A tainting/infection algorithm for a directed graph. Given the start node(s) and time
this algorithm will spread taint/infect any node which has received an edge after this time.
The following node will then infect any of its neighbours that receive an edge after the
time it was first infected. This repeats until the latest timestamp/iterations are met.

## Parameters

 {s}`startTime: Long`
   : Time to start spreading taint

 {s}`infectedNodes: Iterable[String]`
   : List of node names that will start as tainted

 {s}`stopNodes: Iterable[String] = Set()`
   : If set, any node names that will not propagate taint

## States

 {s}`taintStatus: Boolean`
   : {s}`true` if node is infected/tainted

 {s}`taintHistory: List[(String, Long, Long, String)]`
   : List of taint messages received by the vertex. Each message has the format
    {s}`("tainted", edge.ID, event.time, name)`, where {s}`edge.ID` is the `ID` of the edge sending the message,
    {s}`event.time` is the timestamp for the tainting event, and {s}`name` is the name of the source node for the
    message

## Returns

Table of all tainting events

 | vertex name       | propagating edge   | time of event         | source node       |
 | ----------------- | ------------------ | --------------------- | ----------------- |
 | {s}`name: String` | {s}`edge.ID: Long` | {s}`event.time: Long` | {s}`name: String` |