package com.raphtory.internals.serialisers

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.raphtory.internals.components.querymanager.DynamicLoader
import com.typesafe.scalalogging.Logger
import org.apache.bcel.Repository
import org.slf4j.LoggerFactory

import java.io.ByteArrayOutputStream
import scala.util.Using

class DynamicLoaderSerializer(default: Serializer[DynamicLoader]) extends Serializer[DynamicLoader] {

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  /**
    * The class can be loaded either from the classpath via [[Repository]]
    * or from the class storage if the class is not local
    * @param c
    * @tparam T
    * @return
    */
  def class2Bytecode[T](c: Class[T]): (Array[Byte], String) =
    try {
      val jc = Repository.lookupClass(c)

      Using(new ByteArrayOutputStream()) { bos =>
        jc.dump(bos)
        bos.flush()
        bos.toByteArray -> jc.getClassName
      }.get
    }
    catch {
      case t: Throwable =>
        val name = c.getName
        DynamicClassLoader.lookupClass(c.getName) match {
          case Some(bytes) =>
            bytes -> name
          case _           =>
            logger.error(s"Can't find $name in ${DynamicClassLoader.classStorage}")
            throw new IllegalStateException(s"Failed to get bytes for class $name", t)
        }
    }

  override def write(kryo: Kryo, output: Output, q: DynamicLoader): Unit = {
    logger.info(s"Writing down ${q.classes.size} classes for dynamic loading ${q.classes}")
    output.writeInt(q.classes.size)
    q.classes.foreach { c =>
      val (bytes, name) = class2Bytecode(c)
      output.writeString(name)
      output.writeInt(bytes.length)
      output.writeBytes(bytes)
    }
    kryo.writeObject(output, DynamicLoader(), default) // write some dummy obj
    logger.info(s"Done Writing the DynamicLoader object")
  }

  override def read(kryo: Kryo, input: Input, tpe: Class[DynamicLoader]): DynamicLoader = {
    // read how many classes are setup for Dynamic loading
    val n       = input.readInt()
    val classes = (0 until n).map { _ =>
      val name   = input.readString()
      val length = input.readInt()
      val bytes  = input.readBytes(length)
      DynamicClassLoader.injectClass(name, bytes, DynamicClassLoader(kryo.getClassLoader))
    }.toSet
    logger.info(s"Loaded $n classes: $classes")
    kryo.readObject(input, tpe, default).copy(classes = classes) // read the empty dummy obj
  }
}
