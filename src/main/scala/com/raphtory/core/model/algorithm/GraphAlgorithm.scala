package com.raphtory.core.model.algorithm

import net.openhft.hashing.LongHashFunction

abstract class GraphAlgorithm extends Serializable {
  def algorithm(graph:GraphPerspective)

  final def checkID(uniqueChars: String): Long = LongHashFunction.xx3().hashChars(uniqueChars)
}


