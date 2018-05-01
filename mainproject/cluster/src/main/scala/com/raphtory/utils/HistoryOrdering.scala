package com.raphtory.utils

object HistoryOrdering extends Ordering[Long]{
    def compare(key1:Long, key2:Long) = key2.compareTo(key1)
}
