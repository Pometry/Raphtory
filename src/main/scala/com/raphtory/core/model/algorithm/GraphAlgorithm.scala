package com.raphtory.core.model.algorithm

import net.openhft.hashing.LongHashFunction

abstract class GraphAlgorithm extends Serializable {
  def graphStage(graphPerspective: GraphPerspective):GraphPerspective = {graphPerspective}
  def tableStage(graphPerspective: GraphPerspective):Table = {graphPerspective.select(vertex => Row())}
  def write(table: Table): Unit = {}
  def algorithm(graph:GraphPerspective) = {
    write(tableStage(graphStage(graph)))
  }

  final def checkID(uniqueChars: String): Long = LongHashFunction.xx3().hashChars(uniqueChars)
}

