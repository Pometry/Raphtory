`com.raphtory.serialisers.PulsarKryoSerialiser`
(com.raphtory.serialisers.PulsarKryoSerialiser)=
# PulsarKryoSerialiser

 {s}`PulsarKryoSerialiser()`
   : support serialisation and deserialisation using ScalaKryoInstantiator from twitter.chill package

 ## Methods

   {s}`serialise[T](value: T): Array[Byte]`
     : serialise value to byte array

   {s}`deserialise[T](bytes: Array[Byte]): T`
     : deserialise byte array to object

 Example Usage:

```{code-block} scala
import com.raphtory.serialisers.PulsarKryoSerialiser
import com.raphtory.core.config.PulsarController
import org.apache.pulsar.client.api.Schema

val schema: Schema[Array[Byte]] = Schema.BYTES
val kryo = PulsarKryoSerialiser()

val pulsarController = new PulsarController(config)
val client         = pulsarController.accessClient
val producer_topic = "test_lotr_graph_input_topic"
val producer       = client.newProducer(Schema.BYTES).topic(producer_topic).create()
producer.sendAsync(kryo.serialise("Gandalf,Benjamin,400"))
```

```{seealso}
[](com.raphtory.core.client.RaphtoryClient)
```