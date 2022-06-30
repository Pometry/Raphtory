package com.raphtory.internals.serialisers

import com.raphtory.internals.serialisers.DynamicClassLoader.logger
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.io.InputStream
import java.net.URL
import java.util.concurrent.ConcurrentHashMap
import scala.util.Failure
import scala.util.Success
import scala.util.Try

private[serialisers] class DynamicClassLoader(parent: ClassLoader, storage: ConcurrentHashMap[String, Array[Byte]])
        extends ClassLoader(parent) {

  @throws[ClassNotFoundException]
  override def findClass(name: String): Class[_] =
    Try(super.findClass(name)) match {
      case Success(cls)                       => cls
      case Failure(t: ClassNotFoundException) =>
        val clzBytes = storage.get(name)
        if (clzBytes != null) {
          val clz = defineClass(name, clzBytes, 0, clzBytes.length)
          logger.info(s"Successfully injected class $clz")
          clz
        }
        else throw t
    }

  override def findResource(name: String): URL = super.findResource(name)

  override def getResourceAsStream(name: String): InputStream = super.getResourceAsStream(name)

}

object DynamicClassLoader {

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private[serialisers] val classStorage = new ConcurrentHashMap[String, Array[Byte]]()

  def injectClass(name: String, clzBytes: Array[Byte], classLoader: DynamicClassLoader): Class[_] = {
    logger.info(s"Loading dynamic class [$name], size: ${clzBytes.length}")
    classStorage.computeIfAbsent(name, _ => clzBytes)
    classLoader.findClass(name)
  }

  def lookupClass(name: String): Option[Array[Byte]] = Option(classStorage.get(name))

  def apply(classLoader: ClassLoader): DynamicClassLoader =
    new DynamicClassLoader(classLoader, classStorage)
}
