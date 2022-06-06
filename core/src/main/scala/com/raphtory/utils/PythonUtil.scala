package com.raphtory.utils

import scala.jdk.CollectionConverters._

object PythonUtil {

  def toScalaMap[K, V](jm: java.util.Map[K, V]): Map[K, V] =
    jm.asScala.toMap
}
