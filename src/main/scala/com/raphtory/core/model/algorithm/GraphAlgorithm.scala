package com.raphtory.core.model.algorithm

import net.openhft.hashing.LongHashFunction

abstract class GraphAlgorithm extends Serializable {
  def graphStage(graph: GraphPerspective):GraphPerspective = {graph}
  def tableStage(graph: GraphPerspective):Table = {graph.select(vertex => Row())}
  def write(table: Table): Unit = {}
  def algorithm(graph:GraphPerspective) = {
    write(tableStage(graphStage(graph)))
  }

  final def checkID(uniqueChars: String): Long = LongHashFunction.xx3().hashChars(uniqueChars)
}

