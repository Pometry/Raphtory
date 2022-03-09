`com.raphtory.output.FileOutputFormat`
(com.raphtory.output.FileOutputFormat)=
# FileOutputFormat

{s}`FileOutputFormat(filePath: String)`
  : writes output for Raphtory Job and Partition for a pre-defined window and timestamp to File

    {s}`filePath: String`
      : Filepath for writing Raphtory output.

Usage while querying or running algorithmic tests:

```{code-block} scala
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.output.FileOutputFormat
import com.raphtory.core.algorithm.OutputFormat
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.Spout

val graph = Raphtory.createTypedGraph[T](Spout[T], GraphBuilder[T])
val testDir = "/tmp/raphtoryTest"
val outputFormat: OutputFormat = FileOutputFormat(testDir)

graph.pointQuery(EdgeList(), outputFormat, 1595303181, List())
```

 ```{seealso}
 [](com.raphtory.core.algorithm.OutputFormat),
 [](com.raphtory.core.client.RaphtoryClient),
 [](com.raphtory.core.client.RaphtoryGraph),
 [](com.raphtory.core.deploy.Raphtory)
 ```