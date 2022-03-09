`com.raphtory.serialisers.Marshal`
(com.raphtory.serialisers.Marshal)=
# Marshal

{s}`Marshal()`

: support serialisation (`dump`) and deserialisation (`load`) for `ByteArray` input and output streams

## Methods

   {s}`dump[A: ClassTag](o: A): Array[Byte]`
     : Serialise to byte array

       {s}`o: A`
          : object to serialise

   {s}`load[A: ClassTag](buffer: Array[Byte])`
     : Deserialise object from Array[Byte]

       {s}`buffer: Array[Byte]`
          : input buffer

   {s}`deepCopy[A: ClassTag](a: A)`
     : Deep copy

       {s}`a: A`
          : deserialise and persist as deep copy by serialising

Example Usage:

```{code-block} scala
import com.raphtory.serialisers.Marshal
import com.raphtory.core.components.graphbuilder.GraphBuilder

def setGraphBuilder(): GraphBuilder[T]
val graphBuilder = setGraphBuilder()
val safegraphBuilder = Marshal.deepCopy(graphBuilder)
```

```{seealso}
[](com.raphtory.serialisers.PulsarKryoSerialiser)
```