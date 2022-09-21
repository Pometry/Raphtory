package com.raphtory.internals.storage

sealed trait GraphExecutionState {
  def hasMessage(getLocalId: Long): Boolean = ???

  def removeOutEdge(vertexId: Long, sourceId: Long, superstep: Int): Unit = ???

  def removeInEdge(sourceId: Long, vertexId: Long, superstep: Int): Unit = ???

  def removeEdges(vertexId: Long, sourceId: Long, superstep: Int): Unit = ???

  def receiveMessage(vertexId: Any, superstep: Int, data: Any): Unit = ???

  def clearMessages(): Unit = ???

  def nextStep(superStep: Int): Unit = ???

  def isAlive(vertexId: Long): Boolean = false
}

object GraphExecutionState {
  def apply(): GraphExecutionState = new GraphExecutionState {}
}
