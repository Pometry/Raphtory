package com.raphtory.serialisers


/**
  * {s}`Marshal()`
  *
  * : support serialisation (`dump`) and deserialisation (`load`) for `ByteArray` input and output streams
  *
  *
  * ## Methods
  *
  *    {s}`dump[A](o: A)(implicit t: ClassTag[A]): Array[Byte]`
  *      : Serialise to ByteArray
  *
  *        {s}`t: ClassTag[A]`
  *           : serialise object to Array[Byte]
  *
  *    {s}`load[A](buffer: Array[Byte])(implicit expected: ClassTag[A])`
  *      : Deserialise from Array[Byte]
  *
  *        {s}`buffer: Array[Byte]`
  *           : deserialise from byte buffer to object
  *
  *    {s}`deepCopy[A](a: A)(implicit m: reflect.ClassTag[A])`
  *      : Deep copy
  *
  *        {s}`a: ClassTag[A]`
  *           : deserialise and persist as deep copy by serialising
  *
  * Example Usage:
  *
  * ```{code-block} scala
  * import com.raphtory.serialisers.PulsarKryoSerialiser
  * import com.raphtory.core.config.PulsarController
  * import com.raphtory.serialisers.Marshal
  * import com.raphtory.core.components.graphbuilder.GraphBuilder
  *
  * def setGraphBuilder(): GraphBuilder[T]
  * val graphBuilder = setGraphBuilder()
  * val safegraphBuilder = Marshal.deepCopy(graphBuilder)
  * ```
  *
  *
  * ```{seealso}
  * [](com.raphtory.serialisers.PulsarKryoSerialiser)
  * ```
  */
object Marshal {
  import java.io._
  import scala.reflect.ClassTag

  def dump[A](o: A)(implicit t: ClassTag[A]): Array[Byte] = {
    val ba  = new ByteArrayOutputStream(512)
    val out = new ObjectOutputStream(ba)
    out.writeObject(t)
    out.writeObject(o)
    out.close()
    ba.toByteArray
  }

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
  }

  def deepCopy[A](a: A)(implicit m: reflect.ClassTag[A]): A =
    Marshal.load[A](Marshal.dump(a))
}
