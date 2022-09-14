package com.raphtory.internals.serialisers

import com.raphtory.internals.serialisers.DynamicClassLoader.logger
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.io.InputStream
import java.net.URL
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import scala.util.Failure
import scala.util.Success
import scala.util.Try

private[serialisers] class DynamicClassLoader(parent: ClassLoader, storage: ConcurrentHashMap[String, Array[Byte]])
        extends ClassLoader(parent) {
  val loaded = new ConcurrentHashMap[String, Class[_]]()

  @throws[ClassNotFoundException]
  override def findClass(name: String): Class[_] =
    Try(parent.loadClass(name)) match {
      case Success(cls)                       => cls
      case Failure(t: ClassNotFoundException) =>
        loaded.computeIfAbsent(
                name,
                buildFromStorage(exception = t)(_)
        )
    }

  private def buildFromStorage(exception: ClassNotFoundException)(name: String): Class[_] = {
    val clzBytes = storage.get(name)
    if (clzBytes != null) {
      val clz = defineClass(name, clzBytes, 0, clzBytes.length)
      logger.debug(s"Successfully injected class $clz")
      clz
    }
    else throw exception
  }

  override def findResource(name: String): URL = super.findResource(name)

  override def getResourceAsStream(name: String): InputStream = super.getResourceAsStream(name)

}

object DynamicClassLoader {
  private[serialisers] val classStorage = new ConcurrentHashMap[String, Array[Byte]]()
  private val loader                    = new DynamicClassLoader(this.getClass.getClassLoader, classStorage)

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def injectClass(name: String, clzBytes: Array[Byte]): Class[_] = {
    logger.debug(s"Loading dynamic class [$name], size: ${clzBytes.length}")
    classStorage.computeIfAbsent(name, _ => clzBytes)
    loader.findClass(name)
  }

  def lookupClass(name: String): Option[Array[Byte]] = Option(classStorage.get(name))

  def apply(): DynamicClassLoader =
    loader
}
