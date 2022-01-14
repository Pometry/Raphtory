package com.raphtory.core.model.algorithm

import net.openhft.hashing.LongHashFunction

abstract class GraphAlgorithm extends Serializable {
  def algorithm(graph: GraphPerspective):GraphPerspective = {graph}
  def tabularise(graph: GraphPerspective):Table = {graph.select(vertex => Row())}
  def write(table: Table): Unit = {}
  def run(graph:GraphPerspective):Unit = {
    write(tabularise(algorithm(graph)))
  }

  final def checkID(uniqueChars: String): Long = LongHashFunction.xx3().hashChars(uniqueChars)
}

