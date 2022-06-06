package com.raphtory.internals.serialisers

import java.io._
import scala.reflect.ClassTag

/** Support serialisation (`dump`) and deserialisation (`load`) for `ByteArray` input and output streams
  *
  * Usage:
  * {{{
  * import com.raphtory.serialisers.Marshal
  * import com.raphtory.components.graphbuilder.GraphBuilder
  *
  * def setGraphBuilder(): GraphBuilder[T]
  * val graphBuilder = setGraphBuilder()
  * val safegraphBuilder = Marshal.deepCopy(graphBuilder)
  * }}}
  *
  * @see [[KryoSerialiser]]
  */
object Marshal {

  /** Serialise to byte array
    * @param o object to serialise
    */
  def dump[A](o: A)(implicit t: ClassTag[A]): Array[Byte] = {
    val ba  = new ByteArrayOutputStream(512)
    val out = new ObjectOutputStream(ba)
    out.writeObject(t)
    out.writeObject(o)
    out.close()
    ba.toByteArray
  }

  /** Deserialise object from Array[Byte]
    * @param buffer input buffer
    */
  @throws(classOf[IOException])
  @throws(classOf[ClassCastException])
  @throws(classOf[ClassNotFoundException])
  def load[A](buffer: Array[Byte])(implicit expected: ClassTag[A]): A = {
    val in    = new ObjectInputStream(new ByteArrayInputStream(buffer))
    val found = in.readObject.asInstanceOf[ClassTag[_]]
    try {
      found.runtimeClass.asSubclass(expected.runtimeClass)
      in.readObject.asInstanceOf[A]
    }
    catch {
      case _: ClassCastException =>
        in.close()
        throw new ClassCastException(
                "type mismatch;" +
                  "\n found : " + found +
                  "\n required: " + expected
        )
    }
    finally in.close()
  }

  /** Deep copy
    * @param a deserialise and persist as deep copy by serialising
    */
  def deepCopy[A](a: A)(implicit m: reflect.ClassTag[A]): A =
    Marshal.load[A](Marshal.dump(a))
}
