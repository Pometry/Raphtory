package com.raphtory.core.model.algorithm

import net.openhft.hashing.LongHashFunction

abstract class GraphAlgorithm extends Serializable {
  def apply(graph: GraphPerspective):GraphPerspective = {graph}
  def tabularise(graph: GraphPerspective):Table = {graph.select(vertex => Row())}
  def write(table: Table): Unit = {}
  def run(graph:GraphPerspective):Unit = {
    write(tabularise(apply(graph)))
  }

  def ->(graphAlgorithm: GraphAlgorithm):Chain = {Chain(this,graphAlgorithm)}

  final def checkID(uniqueChars: String): Long = LongHashFunction.xx3().hashChars(uniqueChars)
}

