package com.raphtory.internals.python

import scala.jdk.CollectionConverters.MapHasAsScala

object PythonUtil {

  def toScalaMap[K, V](jm: java.util.Map[K, V]): Map[K, V] =
    jm.asScala.toMap
}
