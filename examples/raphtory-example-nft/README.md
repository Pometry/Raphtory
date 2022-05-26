# NFTtory


## Overview
This is an exmaple for running cycles on nft datasets.  

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


