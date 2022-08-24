package com.raphtory.internals.management.python

import com.raphtory.api.analysis.visitor.Vertex

import java.util
import scala.jdk.CollectionConverters.SeqHasAsJava
import com.raphtory.internals.communication.SchemaProviderInstances._

class PythonVertex(v: Vertex) {
  def ID: Any = v.ID

  def name(nameProperty: String): String = v.name(nameProperty)

  def message_queue[T]: util.List[T] = v.messageQueue[T].asJava

  def message_all_neighbours(msg: Any): Unit = v.messageAllNeighbours(msg)

  def message_outgoing_neighbours(msg: Any): Unit = v.messageOutNeighbours(msg)

  def message_incoming_neighbours(msg: Any): Unit = v.messageInNeighbours(msg)

  def set_state(key: String, value: Any): Unit =
    v.setState(key, value)

  def get_state[T](key: String, includeProperties: Boolean): T =
    v.getState[T](key, includeProperties = includeProperties)

  def vote_to_halt(): Unit = v.voteToHalt()

  def out_degree(): Int = v.outDegree

  def in_degree(): Int = v.inDegree

  def degree(): Int = v.degree

  def neighbours(): util.List[Any] = v.neighbours.asJava.asInstanceOf[util.List[Any]]

}
