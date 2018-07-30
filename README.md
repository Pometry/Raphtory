# Raphtory Introduction
Raphtory is an ongoing project to maintain and analyse a temporal graph within a distributed environment. The most recent paper on Raphtory can be found [here](RaphtoryPaper.pdf).

Raphtory is built around the actor model using the [Akka Framework](http://akka.io/). There are two main Actor types, [Routers](cluster/src/main/scala/com.gwz.dockerexp/Actors/RaphtoryActors/RaphtoryRouter.scala) and [Partition Managers](cluster/src/main/scala/com.gwz.dockerexp/Actors/RaphtoryActors/PartitionManager.scala).

Routers take in raw data from a given source and convert it to Graph updates, which may consist of adding, removing or updating the properties of a Vertex or Edge. The Partition Manager in charge of the Vertex (or source of an edge) will then be informed of the update, and will insert it into the ordered history of the affected entities. If the Partition manager is not in charge of all effected entities (such as an edge which spans between two Vertices which are stored in different partitions) it will communicate with other affected partitions to synchronize the update across the cluster.

As Raphtory inserts updates into an ordered history, and no entities are actually deleted (just marked as deleted within their history) the order of execution does not matter. This means that Addition,Deletion and Updating are all addative and cummulative. An overview of this can be seen in the diagram below:

<p align="center">
  <img src="readmepics/raphtory.png" alt="Raphtory diagram"/>
</p>

# Running Raphtory
To make Raphtory as easy to run as possible it has been containerised to be able to run within docker. This means that you only have to [install docker](https://docs.docker.com/engine/installation/) and you will be able to run raphtory on your machine. 

To run Raphtory first clone the [Raphtory-Deployment](https://github.com/miratepuffin/Raphtory-Deployment) project which contains all of the yml files required for running. Once this is cloned and you have docker installed run: 

```bash
docker swarm init  
``` 

This will initialise a docker swarm cluster onto your machine which will allow you to deploy via [docker stack](https://docs.docker.com/engine/reference/commandline/stack/). Docker stack requires a [Docker Compose](https://docs.docker.com/compose/compose-file/) file to specify what containers to run, how many of each type and what ports to open etc. Within the [Raphtory-Deployment](https://github.com/miratepuffin/Raphtory-Deployment) there are two main compose files: One for running on a [cluster](https://github.com/miratepuffin/Raphtory-Deployment/blob/master/docker-compose.yml), one for running [locally](https://github.com/miratepuffin/Raphtory-Deployment/blob/master/docker-compose-local.yml).

##Example workloads to pick from
Once Docker is setup you must pick an example workload to test. There are three to choose from (available [here](https://github.com/miratepuffin/raphtory/tree/master/mainproject/cluster/src/main/scala/com/raphtory/examples)): A 'random' graph where updates are generated at runtime; the [GAB.AI](https://gab.ai/) graph (a graph build from the posts on the social network); and a 'coingraph' build by ingesting the transactions within the bitcoin blockchain. 

Each datasource comes with a [Spout](https://github.com/miratepuffin/raphtory/blob/master/mainproject/cluster/src/main/scala/com/raphtory/core/actors/datasource/UpdaterTrait.scala) which generates/pulls the raw data, a [Router](https://github.com/miratepuffin/raphtory/blob/master/mainproject/cluster/src/main/scala/com/raphtory/core/actors/router/RouterTrait.scala) which parses the data into updates, and example [Live Analysis Managers](https://github.com/miratepuffin/raphtory/blob/master/mainproject/cluster/src/main/scala/com/raphtory/core/actors/analysismanager/LiveAnalysisManager.scala) which peerform some basic processing. Note: Both GAB and Bitcoin workloads have example data baked into Raphtory's base image, meaning you can run these without having access to the posts or a Bitcoin full node established. 

Raphtory uses [Reflection](https://docs.scala-lang.org/overviews/reflection/overview.html) to decide what Spout/Router/Analysis Manager to create at run time. Therefore, to choose one of these examples you do not need to rebuild the Raphtory image, but instead change the docker [environment file](https://docs.docker.com/compose/env-file/) (.env) which docker stack requires when running. Example .env files are available [here](https://github.com/miratepuffin/Raphtory-Deployment/tree/master/EnvExamples) for each of the workloads. For the insturctions below I shall be using the 'fake' bitcoin environment [file](https://github.com/miratepuffin/Raphtory-Deployment/blob/master/EnvExamples/bitcoin_read_dotenv.example). This has several config variables which aren't too important for testing, the main one to focus on are the specified classes highlighted below:

<p align="center">
  <img src="readmepics/bitcoinclasses.png" alt="Raphtory diagram"/>
</p>
 

## Deploy locally via docker-compose/swarm
 Following this we can now run: 

```bash
cp dotenv.example .env
docker stack deploy --compose-file docker-compose.yml
```
This first command will copy the example environment variable file to the default name which docker will look for. The second will then establish the raphtory cluster. Once the cluster is running you can run:

```bash
docker stack ps raphtory
```
which will list all of the containers running within the stack and allow you to connect to their output stream to see what is going on inside. For example to connect to the Partition Manager stream:

```bash
docker service logs raphtory_partitionManager --follow
```  
In addition to these logs [Prometheus](https://prometheus.io/docs/practices/instrumentation/) is running at [http://localhost:8888](http://localhost:8888), providing additional metrics such as CPU/memory utilisation, number of vetices/edges and updates injested. 

Note: the prometheus targets configuration is statically bounded to all the subnet 10.0.0.0/24, as configured for our environments, take a look at your docker swarm networking configuration if metrics should not be available

Finally when you are happy everything is working, the cluster can be removed by running:

```bash
docker stack remove raphtory
```

