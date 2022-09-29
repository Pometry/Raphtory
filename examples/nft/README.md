# What's Awash With NFTs

## Overview
This is the code used to obtain the insights from the Raphtory [NFT blogpost](raphtory.com/nfts/) . 
In this project, we write a Cycle Detection algorithm which finds NFTs that have been sold and 
re-purchased by a previous owner. For more information and our results, please see the [blogpost.](raphtory.com/nfts/)

## Data

Data for this code can be found at the [OSF.IO here](https://osf.io/32zec/)

#### TODO FILL THE OSF DATA LINK ONCE ACCOUNT IS UNBLOCKED 

The data consists of 

1. `Data_API_reduced.csv` - A CSV file containing the Ethereum Transactions of NFT sales. Download and extract this file. 
2. `ETH-USD.csv` - Price of Ethereum/USD pair per day
3. `results.json` - Final results after running the code

The code should automatically download files `1` and `2`. 

## Code

There are two ways to run this example, via scala and python. 

At present the scala code is highly optimised runs in less than 10 seconds. 

### Scala

`src/main/scala/` contains the bulk of the code used in Raphtory. 

- `LocalRunner.scala` runs the example. This initialises and executes the components (FileSpout, GraphBuilder and Algorithm )
  - Runs a FileSpout (reads the Ethereum data from the above CSV file)
  - Executes the `cyclemania` algorithm
  - Saves the result to a `.json` file in the `/tmp/` folder
- `analysis/CycleMania.scala` the cycle detection algorithm

#### Run

Run the `LocalRunner.scala`.

The code will first download the required data files 
```json
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   387  100   387    0     0    912      0 --:--:-- --:--:-- --:--:--   910
  0     0    0     0    0     0      0      0 --:--:--  0:00:01 --:--:--     0
100  109M  100  109M    0     0  18.7M      0  0:00:05  0:00:05 --:--:-- 24.8M
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   387  100   387    0     0   1093      0 --:--:-- --:--:-- --:--:--  1093
  0     0    0     0    0     0      0      0 --:--:--  0:00:01 --:--:--     0
100  130k  100  130k    0     0  90218      0  0:00:01  0:00:01 --:--:-- 90218

```

Next, will convert the csv data into a graph. 

```json
14:54:38.325 [main] INFO  com.raphtory.internals.context.LocalContext$ - Creating Service for 'similar_carmine_rooster'
14:54:38.373 [io-compute-blocker-2] INFO  com.raphtory.internals.management.Prometheus$ - Prometheus started on port /0:0:0:0:0:0:0:0:9999
14:54:39.018 [io-compute-blocker-2] INFO  com.raphtory.internals.components.partition.PartitionOrchestrator$ - Creating '1' Partition Managers for 'similar_carmine_rooster'.
14:54:39.375 [io-compute-blocker-3] INFO  com.raphtory.internals.components.partition.PartitionManager - Partition 0: Starting partition manager for 'similar_carmine_rooster'.
14:54:47.347 [spawner-akka.actor.default-dispatcher-8] INFO  com.raphtory.internals.components.querymanager.QueryManager - Source '0' is unblocking analysis for Graph 'similar_carmine_rooster' with 1077657 messages sent. Latest update time was 1564271447
14:54:47.358 [io-compute-blocker-3] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job CycleMania_4096004696329553927: Starting query progress tracker.
14:54:47.383 [spawner-akka.actor.default-dispatcher-8] INFO  com.raphtory.internals.components.querymanager.QueryManager - Source 0 currently ingested 96.0% of its updates.
14:54:47.384 [spawner-akka.actor.default-dispatcher-8] INFO  com.raphtory.internals.components.querymanager.QueryManager - Query 'CycleMania_4096004696329553927' currently blocked, waiting for ingestion to complete.
14:54:47.440 [spawner-akka.actor.default-dispatcher-9] INFO  com.raphtory.internals.components.querymanager.QueryManager - Source 0 currently ingested 98.0% of its updates.
14:54:47.542 [spawner-akka.actor.default-dispatcher-10] INFO  com.raphtory.internals.components.querymanager.QueryManager - Source 0 has completed ingesting and will now unblock
```

Finally will run the cycle detection algorithm. 

```json
14:54:47.543 [spawner-akka.actor.default-dispatcher-10] INFO  com.raphtory.internals.components.querymanager.QueryManager - Query 'CycleMania_4096004696329553927' received, your job ID is 'CycleMania_4096004696329553927'.
14:54:47.564 [spawner-akka.actor.default-dispatcher-8] INFO  com.raphtory.internals.components.partition.QueryExecutor - CycleMania_4096004696329553927_0: Starting QueryExecutor.
14:54:53.328 [spawner-akka.actor.default-dispatcher-6] INFO  com.raphtory.internals.components.querymanager.QueryHandler - Job 'CycleMania_4096004696329553927': Perspective at Time '1561661534' took 5526 ms to run. 
14:54:53.329 [spawner-akka.actor.default-dispatcher-7] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 'CycleMania_4096004696329553927': Perspective '1561661534' finished in 5972 ms.
14:54:53.330 [spawner-akka.actor.default-dispatcher-7] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job CycleMania_4096004696329553927: Running query, processed 1 perspectives.
14:54:53.335 [spawner-akka.actor.default-dispatcher-6] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job CycleMania_4096004696329553927: Query completed with 1 perspectives and finished in 5978 ms.
```

### Python

`src/main/python` contains
- `nft_analysis.ipynb` this is an interactive jupyter notebook running the same process as above, 
except all the code is written completely in Python. In addition, this notebook also explores the data 
and we find a numerous of wallets suspiciously trading NFTs between each other. 
- `nft_helper.py` are some helper functions containing code to load our results, draw graphs and read the data file. 

Please read the notebook for more informatin or follow the guide in the docs. 

## IntelliJ setup guide

As of February 2022, this is a guide to run this within IntelliJ.

1. From https://adoptopenjdk.net/index.html download OpenJDK 11 (LTS) with the HotSpot VM.
2. Enable this as the project SDK under File > Project Structure > Project Settings > Project > Project SDK.
3. Create a new configuration as an `Application` , select Java 11 as the build, and `com.raphtory.examples.nft.LocalRunner` as the class


