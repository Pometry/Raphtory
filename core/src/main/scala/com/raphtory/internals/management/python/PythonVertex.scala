package com.raphtory.internals.management.python

import com.raphtory.api.analysis.visitor.Vertex

import java.util
import scala.jdk.CollectionConverters.SeqHasAsJava

class PythonVertex(v: Vertex) {
  def ID: Any = v.ID

  def message_queue[T]: util.List[T] = v.messageQueue[T].asJava

  def message_all_neighbours(msg: Any): Unit = v.messageAllNeighbours(msg)

  def set_state(key: String, value: Any): Unit = {
    v.setState(key, value)
  }

  def get_state[T](key: String, includeProperties: Boolean): T = {
    v.getState[T](key, includeProperties = includeProperties)
  }

  def vote_to_halt(): Unit = v.voteToHalt()

}
