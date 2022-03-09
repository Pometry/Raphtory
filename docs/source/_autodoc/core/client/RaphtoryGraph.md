`com.raphtory.core.client.RaphtoryGraph`
(com.raphtory.core.client.RaphtoryGraph)=
# RaphtoryGraph

{s}`RaphtoryGraph`
   : Raphtory Graph extends Raphtory Client to initialise Query Manager, Partitions, Spout Worker
   and GraphBuilder Worker for a deployment ID

 {s}`RaphtoryGraph` should not be created directly. To create a {s}`RaphtoryGraph` use
 {s}`Raphtory.createClient(deploymentID: String = "", customConfig: Map[String, Any] = Map())`.

 The query methods for `RaphtoryGraph` are similar to `RaphtoryClient`

 ## Methods

   {s}`stop()`
     : Stops components - partitions, query manager, graph builders, spout worker

 ```{seealso}
 [](com.raphtory.core.client.RaphtoryClient), [](com.raphtory.core.deploy.Raphtory)
 ```