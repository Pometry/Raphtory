# Ethereum Taint Tracking

## Overview
This example runs a taint tracking algorithm on the subset of the Ethereum blockchain. 

## Project Overview

The `LocalRunner` initiates a FileSpout which reads the Ethereum data from a CSV file. 
The data is a `csv` file (comma-separated values) is located in the `resources` folder. 

Each line contains a transaction delimited via hash, block_hash, block_number, from_address, to_address, value and block_timestamp. 

* `LocalRunner.scala` runs the application including the analysis and outputs to Pulsar.
* `graphbuilder/EthereumGraphBuilder.scala` builds the graph.
* `Distributed.scala` runs the application with in a distributed cluster, tutorial coming soon!
* `Client.scala` connects to a cluster in order to send queries. 
* `analysis/TaintTracking.scala` holds a taint tracking algorithm which retails history.

Refer back to [Building a graph from your data](https://raphtory.readthedocs.io/en/development/Ingestion/sprouter.html#) for a more in-depth explanation on how these parts work to build your graph.

## IntelliJ setup guide

As of February 2022, this is a guide to run this within IntelliJ.

1. From https://adoptopenjdk.net/index.html download OpenJDK 11 (LTS) with the HotSpot VM.
2. Enable this as the project SDK under File > Project Structure > Project Settings > Project > Project SDK.
3. Create a new configuration as an `Application` , select Java 11 as the build, and `com.raphtory.examples.ethereum.LocalRunner` as the class, add the Environment Variables


## Running this example

1. This example project is up on Github: [raphtory-example-ethereum](https://github.com/Raphtory/Examples/tree/0.5.0/raphtory-example-ethereum). Please install dependencies from [here](https://raphtory.readthedocs.io/en/development/Install/installdependencies.html).
2. Download the _transactions_09000000_09099999_small.csv.gz_ data from [raphtory-data](https://github.com/Raphtory/Data) repository and place it inside the 'data' folder.
3. In the Examples folder, open up the directory `raphtory-example-ethereum` to get this example running.
4. Install all the python libraries necessary for visualising your data via the [Jupyter Notebook Tutorial](https://raphtory.readthedocs.io/en/development/PythonClient/tutorial.html).
This example also requires Graphviz and Pygraphviz found [here](https://pygraphviz.github.io/documentation/stable/install.html). 
Once you have Jupyter Notebook up and running on your local machine, you can open up the Jupyter Notebook specific for this project, with all the commands needed to output your graph. This can be found by following the path `python/example.ipynb`.
5. Now you can copy the data from the data folder into `/tmp/data` 
6. You are now ready to run this example. You can either run this example via sbt by running `sbt run` followed by selecting the localrunnder class `com.raphtory.ethereum.LocalRunner` More details for sbt can be found [here](https://raphtory.readthedocs.io/en/development/Install/installdependencies.html#running-raphtory-via-sbt).
7. Once your job has finished, you are ready to go onto Jupyter Notebook and run your analyses/output.

## Output

Output in terminal when running `LocalRunner.scala`
```json
15:52:56.909 [run-main-0] INFO  com.raphtory.ethereum.LocalRunner$ - Starting Ethereum application
15:52:58.064 [run-main-0] INFO  com.raphtory.spouts.FileSpout - Spout: Processing file 'transactions_09000000_09099999.csv.gz' ...
15:52:58.804 [run-main-0] INFO  com.raphtory.core.config.ComponentFactory - Creating '4' Partition Managers.
15:52:59.794 [monix-computation-166] DEBUG com.raphtory.core.components.partition.Reader - Partition 0: Starting Reader Consumer.
15:52:59.848 [monix-computation-171] DEBUG com.raphtory.core.components.partition.Reader - Partition 1: Starting Reader Consumer.
15:52:59.904 [monix-computation-175] DEBUG com.raphtory.core.components.partition.Reader - Partition 2: Starting Reader Consumer.
15:52:59.948 [monix-computation-175] DEBUG com.raphtory.core.components.partition.Reader - Partition 3: Starting Reader Consumer.
15:52:59.952 [run-main-0] INFO  com.raphtory.core.config.ComponentFactory - Creating new Query Manager.
15:53:00.007 [monix-computation-175] DEBUG com.raphtory.core.components.querymanager.QueryManager - Starting Query Manager Consumer.
15:53:00.008 [run-main-0] INFO  com.raphtory.core.client.RaphtoryGraph - Created Graph object with deployment ID 'raphtory_76278785'.
15:53:00.008 [run-main-0] INFO  com.raphtory.core.client.RaphtoryGraph - Created Graph Spout topic with name 'raphtory_data_raw_76278785'.
15:53:00.243 [run-main-0] INFO  com.raphtory.core.config.ComponentFactory - Creating new Query Progress Tracker for  'TaintAlgorithm_1647964380011'.
15:53:00.244 [run-main-0] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Starting query progress tracker.
15:53:00.319 [pulsar-external-listener-3-1] DEBUG com.raphtory.core.components.querymanager.QueryManager - Handling query name: TaintAlgorithm_1647964380011, windows: List(), timestamp: 1575013457, algorithm: com.raphtory.ethereum.analysis.TaintAlgorithm@9a62838
15:53:00.320 [pulsar-external-listener-3-1] INFO  com.raphtory.core.components.querymanager.QueryManager - Point Query 'TaintAlgorithm_1647964380011' received, your job ID is 'TaintAlgorithm_1647964380011'.
15:53:00.609 [monix-computation-175] DEBUG com.raphtory.core.components.querymanager.handler.PointQueryHandler - Job 'TaintAlgorithm_1647964380011': Starting query handler consumer.
15:53:00.692 [monix-computation-175] DEBUG com.raphtory.core.components.partition.QueryExecutor - Job 'TaintAlgorithm_1647964380011' at Partition '1': Starting query executor consumer.
15:53:00.693 [monix-computation-171] DEBUG com.raphtory.core.components.partition.QueryExecutor - Job 'TaintAlgorithm_1647964380011' at Partition '2': Starting query executor consumer.
15:53:00.799 [monix-computation-171] DEBUG com.raphtory.core.components.partition.QueryExecutor - Job 'TaintAlgorithm_1647964380011' at Partition '3': Starting query executor consumer.
15:53:00.807 [monix-computation-175] DEBUG com.raphtory.core.components.partition.QueryExecutor - Job 'TaintAlgorithm_1647964380011' at Partition '0': Starting query executor consumer.
15:53:00.829 [pulsar-external-listener-3-1] DEBUG com.raphtory.core.components.querymanager.handler.PointQueryHandler - Job 'TaintAlgorithm_1647964380011': Spawned all executors in 256ms.
15:53:01.136 [monix-computation-179] DEBUG com.raphtory.core.components.partition.BatchWriter - Partition '0': Processed '100000' messages.
15:53:01.196 [monix-computation-179] DEBUG com.raphtory.core.components.partition.BatchWriter - Partition '1': Processed '100000' messages.
15:53:01.199 [monix-computation-179] DEBUG com.raphtory.core.components.partition.BatchWriter - Partition '2': Processed '100000' messages.
15:53:01.227 [monix-computation-179] DEBUG com.raphtory.core.components.partition.BatchWriter - Partition '3': Processed '100000' messages.
15:53:01.836 [pulsar-external-listener-3-1] DEBUG com.raphtory.core.components.querymanager.handler.PointQueryHandler - Job 'TaintAlgorithm_1647964380011': Perspective 'Perspective(1575013457,None)' is not ready, currently at '0'.
15:53:01.861 [monix-computation-179] DEBUG com.raphtory.core.components.partition.BatchWriter - Partition '0': Processed '200000' messages.
15:53:02.039 [monix-computation-179] DEBUG com.raphtory.core.components.partition.BatchWriter - Partition '3': Processed '200000' messages.
```

Terminal output to show job has finished and is ready for visualising the data in Jupyter Notebook
```json
15:56:07.574 [pulsar-external-listener-4-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job TaintAlgorithm_1647964553172: Running query, processed 1 perspectives.
15:56:07.574 [pulsar-external-listener-3-1] DEBUG com.raphtory.core.components.partition.QueryExecutor - Job 'TaintAlgorithm_1647964553172' at Partition '2': Received 'EndQuery' message.
15:56:07.584 [pulsar-external-listener-4-1] DEBUG com.raphtory.core.components.partition.QueryExecutor - Job 'TaintAlgorithm_1647964553172' at Partition '3': Received 'EndQuery' message.
15:56:07.593 [pulsar-external-listener-4-1] DEBUG com.raphtory.core.components.partition.QueryExecutor - Job 'TaintAlgorithm_1647964553172' at Partition '1': Received 'EndQuery' message.
15:56:07.593 [pulsar-external-listener-4-1] INFO  com.raphtory.core.components.querytracker.QueryProgressTracker - Job TaintAlgorithm_1647964553172: Query completed with 1 perspectives and finished in 14201 ms.
15:56:13.851 [monix-computation-181] DEBUG com.raphtory.core.components.partition.LocalBatchHandler - Spout: Scheduling spout to poll again in 10 seconds.
```

The results will be saved to the topics `TaintTracking`, producing the following results

Taint Tracking sample dataframe which shows the following columns 
* runtime - time the algorithm was run
* infected - node that was infected
* infectedBy - who the node was infected by 
* infectionTime  - time of infection
* infectedAtHash  - transaction hash of infected
* value - value of infection

```json
1575013457,0x137ad9c4777e1d36e4b605e745e8f37b2b62e9c5,0xb0ae3f114b6b2727b3ed7e2f6747efc6088db8f9,1574935818,0x5e0951274398a9b93168538647eab363d6d6686e368d8019437a5df7e60dac93,3.998E21
1575013457,0x137ad9c4777e1d36e4b605e745e8f37b2b62e9c5,0xb0ae3f114b6b2727b3ed7e2f6747efc6088db8f9,1575013233,0x5e0951274398a9b93168538647eab363d6d6686e368d8019437a5df7e60dac93,3.998E21
1575013457,0x268eac6cf51c5c780b0f68f67c53e9f4e4a06ade,0xa09871aeadf4994ca12f5c0b6056bbd1d343c029,1574933919,0xa338724a0639f95f30d80c031499077d2d25de69640819a1af338d5cb218b4e6,2.0169E15
1575013457,0x3408edca2d47ddaa783a3563d991b8ddebcd973b,0x39925dd94671970b23220cbd5b1e8ed5dd521f15,1574948900,0xa072f80dc8e6764074af90acc79bd1f9acb9661342c18a983dc074358fed4903,9.0E20
...
```

