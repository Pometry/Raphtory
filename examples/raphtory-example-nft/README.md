# What's Awash With NFTs

## Overview
This is the code used to obtain the insights from the Raphtory [NFT blogpost](raphtory.com/nfts/) . 
In this project, we write a Cycle Detection algorithm which finds NFTs that have been sold and 
re-purchased by a previous owner. For more information and our results, please see the [blogpost.](raphtory.com/nfts/)

## Data

Data for this code can be found at the [OSF.IO here]()

#### TODO FILL THE OSF DATA LINK ONCE ACCOUNT IS UNBLOCKED 

The data consists of 

1. `Data_API_clean_nfts_ETH_only.csv.gz` - A CSV file containing the Ethereum Transactions of NFT sales. Download and extract this file. 
2. `ETH-USD.csv` - Price of Ethereum/USD pair per day
3. `results_nft_cycles.json` - Final results after running the code

The code expects files `1` and `2` to be found under the `/tmp` directory. 
If you change this, then adjust the code accordingly. 

## Code

### Scala

`src/main/scala/` contains the bulk of the code used in Raphtory. 

- `LocalRunner.scala` runs the example. This initialises and executes the components (FileSpout, GraphBuilder and Algorithm )
  - Runs a FileSpout (reads the Ethereum data from the above CSV file)
- `NFTGraphBuilder.scala` splits the data and builds this into a Raphtory Bipartide Graph
- `CycleMania.scala` runs our cycle detection algorithm 

### Python

`python` contains the helper scripts we used to analyse the data post results. 

`cycle_analysis.ipynb` shows our thought process as we explore the results we obtained from Raphtory.


## IntelliJ setup guide

As of February 2022, this is a guide to run this within IntelliJ.

1. From https://adoptopenjdk.net/index.html download OpenJDK 11 (LTS) with the HotSpot VM.
2. Enable this as the project SDK under File > Project Structure > Project Settings > Project > Project SDK.
3. Create a new configuration as an `Application` , select Java 11 as the build, and `com.raphtory.nft.ethereum.LocalRunner` as the class, add the Environment Variables


