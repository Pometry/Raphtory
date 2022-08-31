# Raphtory Streaming and Apache Pulsar

As was briefly mentioned in the introduction, alongside the `Raphtory.load()` function for static datasets, Raphtory has a streaming graph deployment (`Raphtory.stream()`) which allows the continuous ingestion of new data. From an API level this looks almost identical and all spouts that work for `load` will work in a `stream`. For instance, the LOTR `TutorialRunner` we previously worked with can be edited to run as a stream: 

```
val builder = new LOTRGraphBuilder()	
val graph   = Raphtory.load(spout = source, graphBuilder = builder)		     	 val graph   = Raphtory.stream(spout = source, graphBuilder = builder)
```

 Whilst the `FileSpout` can be set to poll a directory for new files in this instance, more interestingly, we can now make use of an array of new data input formats including pub/sub systems like [Kafka](https://kafka.apache.org) and online streams like the [Twitter API](https://developer.twitter.com/en/docs/twitter-api/tools-and-libraries/v2). In these instances, new data can continuously flow into Raphtory as soon as it is published and queries can be set to run on it as soon as it has synchronised. 
 
 To manage parallel ingestion and analysis safely Raphtory maintains a global watermark which reports the status of each partition, and the time they believe is safe to execute on. These individual times are then aggregated into a global minimum time to make sure only safe Perspectives are generated and, therefore, the output results are correct. 
 
```{note}
The query API, execution and results on streaming graphs are exactly the same as those on their loaded counterparts assuming the same graph builder and underlying data.
```
 
Whilst the API may be very similar, under the hood things are very different. When deployed as a stream, all of the components which orchestrate ingestion and analysis are actually running as independent services. This includes the `Spout`, `Graph Builder`, `Partition Manager` and `Query Manager`. This is the way Raphtory was designed to run in production, where each service may be multiplied and distributed across many machines allowing us to drastically increase the size of the graphs we analyze.

```{note}
We can increase the number of partitions we have locally to test how our analysis will run in a distributed environment. This can be set in the application.conf under `raphtory.partitions.countPerServer` respectfully, or by exporting the `RAPHTORY_PARTITIONS_COUNTPERSERVER` environment variables.
```

To support distribution, Raphtory has been rebuilt to utilize [Akka](https://akka.io) for control messages and [Apache Pulsar](https://pulsar.apache.org) as its broker to support communication between components. Using Pulsar means that all data sent within Raphtory is fully backed up and communication can easily scale to millions of messages a second. 

This does, however, mean we need to be able to connect to a Pulsar cluster when running Raphtory in this fashion. Fortunately, Pulsar comes with a standalone mode which packages everything together to be run once installed.  Pulsar can be installed in a variety of ways, which can be seen below. 

We have also created a handy Pulsar script which will automatically manage this for you, if you prefer. The details for this can be found at the bottom of the page.


## Installing Apache Pulsar

### Downloading Natively
The simplest way of getting Pulsar is to download it from the Apache archive and unzip the tar. 

```bash
wget https://archive.apache.org/dist/pulsar/pulsar-2.9.0/apache-pulsar-2.9.0-bin.tar.gz 
tar -xzf apache-pulsar-2.9.0-bin.tar.gz
```

Once done, you may `cd` into the Pulsar directory and run the executable in standalone mode. This will launch Apache Zookeeper, Bookkeeper and Pulsar in one service and provide everything we need to run Raphtory.

```bash
cd apache-pulsar-2.9.0
bin/pulsar standalone
```

### Running in Docker
The alternative way of running Pulsar is to do so inside a virtual environment, the easiest to use being [Docker](https://www.docker.com). 

```{note}
For Mac users with the new M1 chips, this is currently the only way of running Pulsar. 
```

The simplest way to install Docker is to use the [Desktop client](https://www.docker.com/products/docker-desktop) which is available for Mac, Linux and Windows. Once you have downloaded and installed Docker Desktop it will start up and you will be presented with a dashboard which shows you currently have no `containers` (services) running:

<p align="center">
	<img src="../_static/install/dockerdesktop.png" width="80%" alt="Sdk Man java lists"/>
</p>

Lets now run a container for Pulsar:

```
docker run -it \ 
-p 6650:6650 \
-p 8080:8080 \
-p 2181:2181 \
-v $PWD/data:/pulsar/data \
apachepulsar/pulsar:2.9.0 \
bin/pulsar standalone
```

Here we run the docker container in an interactive mode `run -it` where we attach to the service and can see the output. We bind the ports for Zookeeper and Pulsar to our localhost, so that Raphtory can communicate with the service. We create a volume called `data` in the current working directory which will contain everything written into Pulsar `$PWD/data:/pulsar/data`. Finally we give the docker image (which contains all the code) `apachepulsar/pulsar:2.9.0` and tell it to run the same command as if we run it natively (`bin/pulsar standalone`).

### What it should look like
Pulsar standalone produces _A LOT_ of output, which can almost exclusively be ignored. It will take about a minute to start up and you will know when this is completed as it stops outputting and prints a block of JSON telling you its address. 

<p align="center">
	<img src="../_static/install/pulsarfinished.png" width="80%" alt="Sdk Man java lists"/>
</p>

If you run Pulsar in Docker, you will now be able to see your container in the dashboard, which is given a random name, for example, mine was called wonderful_fermat. 

<p align="center">
	<img src="../_static/install/dockerrunning.png" width="80%" alt="Sdk Man java lists"/>
</p>

**Note** If you have any issues with Pulsar (errors etc.) the quickest solution is often to delete the data folder and allow it to restart from scratch. This won't affect Raphtory as most data is recreated on new runs anyway. 


## Our Pulsar install and run scripts

If you don't want to manage pulsar yourself you can instead use our script for it which comes as part of the Raphtory repository you have already cloned. Simply copy to commands below under your preferred way of running pulsar and everything will be up in no time.

```{note}
With the below:
- You can replace /usr/local/bin with the install path you would like.
- You can replace 2.9.0 with another pulsar version if you require another version.
- If you are missing dependencies, the script will report what is missing
- If you wish to know more about the script options run `./bin/pulsar-local`
```
<br>

### Running Pulsar locally using java process
|   |   |
|---|---|
|Checkout the Raphtory code base | `git clone URL && cd Raphtory`|
|Install binaries | `./bin/pulsar-local -d machine -a install -v 2.9.0 -p /usr/local/bin`|
|Install pulsar connectors | `./bin/pulsar-local -d machine -a install-connectors -v 2.9.0 -p /usr/local/bin`|
|Start pulsar | `./bin/pulsar-local -d machine -a start -v 2.9.0 -p /usr/local/bin`|
|Stop pulsar | `./bin/pulsar-local -d machine -a stop -v 2.9.0`|

<br>

### Running Pulsar locally using docker
|   |   |
|---|---|
|Checkout the Raphtory code base | `git clone URL && cd Raphtory`|
|Start pulsar docker container |`./bin/pulsar-local -d docker -a start -v 2.9.0`|
|Stop pulsar docker container |`./bin/pulsar-local -d docker -a stop -v 2.9.0`|
|Get pulsar docker container logs | `./bin/pulsar-local -d docker -a logs -v 2.9.0`|

<br>

### Running Pulsar locally using minikube (must have existing minikube cluster)
|   |   |
|---|---|
|Checkout the Raphtory code base | `git clone URL && cd Raphtory`|
|Get kube context of your existing cluster | `kubectl config get-contexts -o name`|
|To start pulsar on minikube | `./bin/pulsar-local -d minikube -a start -v 2.9.0 -m <minikube_context_name>`|
|To stop running pulsar deployment on minikube | `./bin/pulsar-local -d minikube -a start -v 2.9.0 -m <minikube_context_name>`|

<br>