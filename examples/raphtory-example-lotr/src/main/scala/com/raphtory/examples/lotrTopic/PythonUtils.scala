package com.raphtory.examples.lotrTopic

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.typeOf

object PythonUtils {

  def toScalaMap[K, V](jm: java.util.Map[K, V]): Map[K, V] = {
    jm.asScala.toMap
  }

//    val x = typeOf("x")
}
