package com.raphtory.core.implementations.pojograph.entities.internal

object HistoryOrdering extends Ordering[Long] {
  def compare(key1: Long, key2: Long) = key2.compareTo(key1)
}