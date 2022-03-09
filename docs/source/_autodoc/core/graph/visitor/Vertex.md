`com.raphtory.core.graph.visitor.Vertex`
(com.raphtory.core.graph.visitor.Vertex)=
# Vertex

{s}`Vertex`
  : Extends [{s}`EntityVisitor`](com.raphtory.core.graph.visitor.EntityVisitor) with vertex-specific functionality

The {s}`Vertex` is the main entry point for exploring the graph using a
[{s}`GraphAlgorithm](com.raphtory.core.algorithm.GraphAlgorithm) given the node-centric nature of Raphtory.
It provides access to the edges of the graph and can send messages to and receive messages from other vertices.
A {s}`Vertex` can also store computational state.

## Attributes

{s}`ID(): Long`
  : vertex ID

{s}`name(nameProperty: String = "name"): String`
  : get the name of the vertex

    {s}`nameProperty: String = "name"`
      : vertex property to use as name (should uniquely identify the vertex)

    if {s}`nameProperty` does not exist, this function returns the string representation of the vertex ID

{s}`inDegree: Int`
  : number of in-neighbours of the vertex

{s}`outDegree: Int`
  : number of out-neighbours of the vertex

{s}`degree: Int`
  : total number of neighbours (including in-neighbours and out-neighbours) of the vertex

{s}`weightedInDegree[A, B](weightProperty: String = "weight", edgeMergeStrategy: Seq[(Long, A)] => B = PropertyMergeStrategy.sum[A], defaultWeight: A = 1): B`
  : sum of incoming edge weights

    For the meaning of the input arguments see the
    [{s}`Edge.weight` documentation](com.raphtory.core.graph.visitor.Edge).

{s}`weightedOutDegree[A, B](weightProperty: String = "weight", edgeMergeStrategy: Seq[(Long, A)] => B = PropertyMergeStrategy.sum[A], defaultWeight: A = 1): B`
  : sum of outgoing edge weights

    For the meaning of the input arguments see the
    [{s}`Edge.weight` documentation](com.raphtory.core.graph.visitor.Edge).

{s}`weightedTotalDegree[A, B](weightProperty: String = "weight", edgeMergeStrategy: Seq[(Long, A)] => B = PropertyMergeStrategy.sum[A], defaultWeight: A = 1): B`
  : sum of incoming and outgoing edge weights

    For the meaning of the input arguments see the
    [{s}`Edge.weight` documentation](com.raphtory.core.graph.visitor.Edge).

## Neighbours and Edges

{s}`getAllNeighbours(after: Long = 0L, before: Long = Long.MaxValue): List[Long]`
  : get IDs of all in- and out-neighbours of the vertex

    {s}`after: Long = 0L`
      : only return neighbours that are active after time {s}`after`

    {s}`before: Long = Long.MaxValue`
      : only return neighbours that are active before time {s}`before`

{s}`getOutNeighbours(after: Long = 0L, before: Long = Long.MaxValue): List[Long]`
  : get IDs of all out-neighbours of the vertex

    {s}`after: Long = 0L`
      : only return neighbours that are active after time {s}`after`

    {s}`before: Long = Long.MaxValue`
      : only return neighbours that are active before time {s}`before`

{s}`getInNeighbours(after: Long = 0L, before: Long = Long.MaxValue): List[Long]`
  : get IDs fo all in-neighbours of the vertex

    {s}`after: Long = 0L`
      : only return neighbours that are active after time {s}`after`

    {s}`before: Long = Long.MaxValue`
      : only return neighbours that are active before time {s}`before`

{s}`isNeighbour(id: Long): Boolean`
  : check if the vertex with ID {s}`id` is an in- or out-neighbour of this vertex

{s}`isInNeighbour(id: Long): Boolean`
  : check if the vertex with ID {s}`id` is an in-neighbour of this vertex

{s}`isOutNeighbour(id: Long): Boolean`
  : check if the vertex with ID {s}`id` is an out-neighbour of this vertex

{s}`getEdges(after: Long = 0L, before: Long = Long.MaxValue): List[Edge]`
  : return all edges starting or ending at this vertex

    {s}`after: Long = 0L`
      : only return edges that are active after time {s}`after`

    {s}`before: Long = Long.MaxValue`
      : only return edges that are active before time {s}`before`

{s}`getOutEdges(after: Long = 0L, before: Long = Long.MaxValue): List[Edge]`
  : return all edges starting at this vertex

    {s}`after: Long = 0L`
      : only return edges that are active after time {s}`after`

    {s}`before: Long = Long.MaxValue`
      : only return edges that are active before time {s}`before`

{s}`getInEdges(after: Long = 0L, before: Long = Long.MaxValue): List[Edge]`
  : return all edges ending at this vertex

    {s}`after: Long = 0L`
      : only return edges that are active after time {s}`after`

    {s}`before: Long = Long.MaxValue`
      : only return edges that are active before time {s}`before`

{s}`getEdge(id: Long, after: Long = 0L, before: Long = Long.MaxValue): Option[Edge]`
  : return specified edge if it is an in- or out-edge of this vertex

    {s}`id: Long`
      : ID of edge to return

    {s}`after: Long = 0L`
      : only return edge if it is active after time {s}`after`

    {s}`before: Long = Long.MaxValue`
      : only return edge if it is active before time {s}`before`

{s}`getOutEdge(id: Long, after: Long = 0L, before: Long = Long.MaxValue): Option[Edge]`
  : return specified edge if it is an out-edge of this vertex

    {s}`id: Long`
      : ID of edge to return

    {s}`after: Long = 0L`
      : only return edge if it is active after time {s}`after`

    {s}`before: Long = Long.MaxValue`
      : only return edge if it is active before time {s}`before`

{s}`getInEdge(id: Long, after: Long = 0L, before: Long = Long.MaxValue): Option[Edge]`
  : return specified edge if it is an in-edge of this vertex

    {s}`id: Long`
      : ID of edge to return

    {s}`after: Long = 0L`
      : only return edge if it is active after time {s}`after`

    {s}`before: Long = Long.MaxValue`
      : only return edge if it is active before time {s}`before`

{s}`explodeEdges(after: Long = 0L, before: Long = Long.MaxValue): List[ExplodedEdge]`
  : return exploded [{s}`ExplodedEdge`](com.raphtory.core.graph.visitor.ExplodedEdge) views for each time point
    that an in- or out-edge of this vertex is active

    {s}`after: Long = 0L`
      : only return views for activity after time {s}`after`

    {s}`before: Long = Long.MaxValue`
      : only return view for activity before time {s}`before`

{s}`explodeOutEdges(after: Long = 0L, before: Long = Long.MaxValue): List[ExplodedEdge]`
  : return exploded [{s}`ExplodedEdge`](com.raphtory.core.graph.visitor.ExplodedEdge) views for each time point
    that an out-edge of this vertex is active

    {s}`after: Long = 0L`
      : only return views for activity after time {s}`after`

    {s}`before: Long = Long.MaxValue`
      : only return view for activity before time {s}`before`

{s}`explodeInEdges(after: Long = 0L, before: Long = Long.MaxValue): List[ExplodedEdge]`
  : return exploded [{s}`ExplodedEdge`](com.raphtory.core.graph.visitor.ExplodedEdge) views for each time point
    that an in-edge of this vertex is active

    {s}`after: Long = 0L`
      : only return views for activity after time {s}`after`

    {s}`before: Long = Long.MaxValue`
      : only return view for activity before time {s}`before`

{s}`explodeEdge(id: Long, after: Long = 0L, before: Long = Long.MaxValue): Option[List[ExplodedEdge]]`
  : return exploded [{s}`ExplodedEdge`](com.raphtory.core.graph.visitor.ExplodedEdge) views for an individual edge
    if it is an in- or out-edge of this vertex

    {s}`id: Long`
      : ID of edge to explode

    {s}`after: Long = 0L`
      : only return views for activity after time {s}`after`

    {s}`before: Long = Long.MaxValue`
      : only return view for activity before time {s}`before`

{s}`explodeOutEdge(id: Long, after: Long = 0L, before: Long = Long.MaxValue): Option[List[ExplodedEdge]]`
  : return exploded [{s}`ExplodedEdge`](com.raphtory.core.graph.visitor.ExplodedEdge) views for an individual edge
    if it is an out-edge of this vertex

    {s}`id: Long`
      : ID of edge to explode

    {s}`after: Long = 0L`
      : only return views for activity after time {s}`after`

    {s}`before: Long = Long.MaxValue`
      : only return view for activity before time {s}`before`

{s}`explodeInEdge(id: Long, after: Long = 0L, before: Long = Long.MaxValue): Option[List[ExplodedEdge]]`
  : return exploded [{s}`ExplodedEdge`](com.raphtory.core.graph.visitor.ExplodedEdge) views for an individual edge
    if it is an in-edge of this vertex

    {s}`id: Long`
      : ID of edge to explode

    {s}`after: Long = 0L`
      : only return views for activity after time {s}`after`

    {s}`before: Long = Long.MaxValue`
      : only return view for activity before time {s}`before`

## Messaging

{s}`voteToHalt(): Unit`
  : vote to stop iterating (iteration stops if all vertices voted to halt)

{s}`hasMessage(): Boolean`
  : check if vertex has received messages

{s}`messageQueue[T]: List[T]`
  : queue of received messages

    {s}`T`
      : message data type

{s}`messageSelf(data: Any): Unit`
  : send data to this vertex at the next Step/Iteration

    {s}`data: Any`
      : message data to send

{s}`messageVertex(vertexId: Long, data: Any): Unit`
  : send data to another vertex at next Step/Iteration

    {s}`vertexID: Long`
      : ID of target vertex for the message

    {s}`data: Any`
      : message data to send

{s}`messageAllNeighbours(message: Any): Unit`
  : send the same message data to all in- and out-neighbours of this vertex

    {s}`message: Any`
      : message data to send

{s}`messageOutNeighbours(message: Any): Unit`
  : send the same message data to all out-neighbours of this vertex

    {s}`message: Any`
      : message data to send

{s}`messageInNeighbours(message: Any): Unit`
  : send the same message data to all in-neighbours of this vertex

    {s}`message: Any`
      : message data to send

## Algorithmic State

{s}`setState(key: String, value: Any): Unit`
  : set algorithmic state for this vertex

    {s}`key: String`
      : key to use for setting value

    {s}`value: Any`
      : new value for state

{s}`getState[T](key: String, includeProperties: Boolean = false): T`
  : retrieve value from algorithmic state

    {s}`T`
      : value type for state

    {s}`key: String`
      : key to use for retrieving state

    {s}`includeProperties: Boolean = false`
      : set this to {s}`true` to fall-through to vertex properties if {s}`key` is not found

{s}`getStateOrElse[T](key: String, value: T, includeProperties: Boolean = false): T`
 : retrieve value from algorithmic state if it exists or return a default value otherwise

    {s}`T`
      : value type for state

    {s}`key: String`
      : key to use for retrieving state

    {s}`value: T`
      : default value to return if state does not exist

    {s}`includeProperties: Boolean = false`
      : set this to {s}`true` to fall-through to vertex properties if {s}`key` is not found in
        algorithmic state

{s}`getOrSetState[T](key: String, value: T, includeProperties: Boolean = false): T`
  : retrieve value from  algorithmic state if it exists, otherwise set state to default value and return it

    {s}`T`
      : value type for state

    {s}`key: String`
      : key to use for retrieving state

    {s}`value: T`
      : default value to return if state does not exist

    {s}`includeProperties: Boolean = false`
      : Set this to {s}`true` to fall-through to vertex properties if {s}`key` is not found in
        algorithmic state. If the value is pulled in from properties, the new value is set as state.

{s}`appendToState[T: ClassTag](key: String, value: T): Unit`
  : append new value to existing array or initialise new array if state does not exist

    The value type of the state is assumed to be {s}`Array[T]` if the state already exists.

    {s}`T`
      : value type for state (needs to have a `ClassTag` available due to Scala {s}`Array` implementation)

    {s}`key: String`
      : key to use for retrieving state

    {s}`value: T`
      : value to append to state

{s}`containsState(key: String, includeProperties: Boolean = false): Boolean`
  : check if algorithmic state with key {s}`key` exists

    {s}`includeProperties: Boolean = false`
      : Set this to {s}`true` to fall-through to vertex properties if {s}`key` is not found.
        If set, this function only returns {s}`false` if {s}`key` is not included in either algorithmic state
        or vertex properties