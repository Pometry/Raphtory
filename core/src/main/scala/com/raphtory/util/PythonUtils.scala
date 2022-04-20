package com.raphtory.util

import scala.collection.JavaConverters._

class PythonUtils {

  def toScalaMap[K, V](jm: java.util.Map[K, V]): Map[K, V] = {
    jm.asScala.toMap
  }

}
