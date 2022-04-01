package com.raphtory.serialisers

/**
  * {s}`Marshal()`
  *
  * : support serialisation (`dump`) and deserialisation (`load`) for `ByteArray` input and output streams
  *
  * ## Methods
  *
  *    {s}`dump[A: ClassTag](o: A): Array[Byte]`
  *      : Serialise to byte array
  *
  *        {s}`o: A`
  *           : object to serialise
  *
  *    {s}`load[A: ClassTag](buffer: Array[Byte])`
  *      : Deserialise object from Array[Byte]
  *
  *        {s}`buffer: Array[Byte]`
  *           : input buffer
  *
  *    {s}`deepCopy[A: ClassTag](a: A)`
  *      : Deep copy
  *
  *        {s}`a: A`
  *           : deserialise and persist as deep copy by serialising
  *
  * Example Usage:
  *
  * ```{code-block} scala
  * import com.raphtory.serialisers.Marshal
  * import com.raphtory.components.graphbuilder.GraphBuilder
  *
  * def setGraphBuilder(): GraphBuilder[T]
  * val graphBuilder = setGraphBuilder()
  * val safegraphBuilder = Marshal.deepCopy(graphBuilder)
  * ```
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
    finally in.close()
  }

  def deepCopy[A](a: A)(implicit m: reflect.ClassTag[A]): A =
    Marshal.load[A](Marshal.dump(a))
}
