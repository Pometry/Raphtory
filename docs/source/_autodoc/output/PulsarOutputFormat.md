`com.raphtory.output.PulsarOutputFormat`
(com.raphtory.output.PulsarOutputFormat)=
# PulsarOutputFormat

{s}`PulsarOutputFormat(pulsarTopic: String)`
  : writes output output to a Raphtory Pulsar topic

    {s}`pulsarTopic: String`
      : Topic name for writing to Pulsar.

Usage while querying or running algorithmic tests:

```{code-block} scala
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.output.PulsarOutputFormat
import com.raphtory.core.algorithm.OutputFormat
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.Spout

val graph = Raphtory.createGraph[T](Spout[T], GraphBuilder[T])
val outputFormat: OutputFormat = PulsarOutputFormat("EdgeList")

graph.pointQuery(EdgeList(), outputFormat, 1595303181, List())
```

 ```{seealso}
 [](com.raphtory.core.algorithm.OutputFormat),
 [](com.raphtory.core.client.RaphtoryClient),
 [](com.raphtory.core.client.RaphtoryGraph),
 [](com.raphtory.core.deploy.Raphtory)
 ```