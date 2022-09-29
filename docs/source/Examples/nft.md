# What's Awash With NFTs: Finding Suspicious Trading Cycles

## Overview

In this example we will use Raphtory to find users that have been trading NFTs in cycles. At the end of this example, you will have been able to: 
- read a CSV into raphtory bipartite graph
- run the cycle detection algorithm
- analyse the results to find a list of traders suspiciously trading NFTs

The example is presented in both in Scala and Python.
This is the code used to obtain the insights from the Raphtory [NFT blogpost](raphtory.com/nfts/) .

## Pre-requisites

Follow our Installation guide: [Scala](../Install/scala/install.md) or [Python (with Conda)](../Install/python/install_conda.md), [Python (without Conda)](../Install/python/install_no_conda.md).

## Background

A Non-fungible Token (NFT) is a digital asset which cannot be copied, substituted, or subdivided, meaning it is wholly unique.

![title](http://web.archive.org/web/20220328080727im_/https://blog.portion.io/content/images/2021/07/Kevin-McCoy-Quantum.gif)

NFT’s are stored on blockchain systems, notably Ethereum, with the owner of an NFT being whomever has access to the wallet it is stored within.

Since the first NFT, Quantum (shown above), many new NFT artworks have arisen including Cryptokitties and Cryptopunks. This culminated in a boom in 2021 which included the arrival of the Bored Ape Yacht Club, with NFTS reaching a global market valued of $15.7 Billion.

### Set your tokens to spun

With such large amounts moving around in a seemingly unregulated market this has naturally attracted criminals looking to get in on the action. For instance, a recent crime report from [Chainalysis](https://blog.chainalysis.com/reports/2022-crypto-crime-report-preview-nft-wash-trading-money-laundering/) revealed that 262 users have been participating in wash-trading their NFTs.

A wash-trade occurs when the buyer and seller within a transaction are the same person or group of people. This is normally done to mislead potential buyers into thinking the asset is worth more, by pumping up the price with each trade, or to make them believe it has very high liquidity, i.e., if they buy it, it can be quickly sold at any point in the future. In reality when they do buy it, they find they cannot sell it to anyone for remotely close to the price they purchased it for. 

The authorities have been hot on the heels of NFT crime, where just last month a former NFT marketplace employee was charged with the [first case of digital asset insider trading](https://www.justice.gov/usao-sdny/pr/former-employee-nft-marketplace-charged-first-ever-digital-asset-insider-trading-scheme). This won't stop criminals from attempting to make money off NFT's, but it will force them to attempt to better obscure their activities. 

This got us thinking. Network of transactions... occurring over time... complex patterns hidden in large volumes of data... if only we had a tool to let us investigate further :thinking:! Fortunately, Raphtory was at hand to let us dive into the problem. 

### What are we digging into?

With the above premise in mind, we set out to see what sort of patterns we could find within the network of trades, specifically focusing on `cycles`. This is when a NFT has been sold by a wallet, but eventually ends up being owned by it again. This can involve a chain of any number of other wallets, take any amount of time, and increase or decrease the price by any amount.

Below we can see an example of such a cycle where `Wallet A` sold an NFT to `Wallet B` for `$50` on Monday, but its back in their position by Thursday, costing them `$750`. They either have serious sellers remorse or, more likely, the same person owns all these wallets and is trying to pump up the price of the given NFT.

![](https://www.raphtory.com/images/nfts/NFT_Cycle.png)
*Example NFT cycle across 5 wallets over the course of a week*

This example opens many initial questions about the network which we will explore, such as: 
* Do users regularly cycle NFTs between each other? And if so, is cycling localised to a small number of wallets or is the practice widespread?
* What is the timespan over which these normally take place?
* Can we actually see typical washing practices like `pumping` in action? 

### We have our tools, where's the data?

To answer these questions we used open data from the ["Mapping the NFT revolution: market trends, trade networks, and visual features"](https://doi.org/10.1038/s41598-021-00053-8) paper.
This dataset contains 4.7 million NFTs that in total were traded across 6.1 million transactions between June 23, 2017, and April 27, 2021. The data we used in in this tutorial is a cleaned and trimmed version.

### Modelling the data 

Prior research has analysed this data as a network of wallets, however, we took a different approach. 
We created a `bipartite graph`, meaning a graph with two types of nodes, corresponding to wallets (addresses) and NFTs. Directed edges $Wallet\rightarrow NFT$ in this network indicate a purchase. 

From the perspective of an individual NFT within this model we can see all the wallets which have purchased it, when these occurred and for how much. With this information it is trivial to rebuild cycles of any length. 

Below is an example of a bipartite graph. On the left we have Ethereum wallets, the right NFTs, and the edges indicating who has purchased what. Some of these transactions (in green) appear to be innocuous, whereas others (in red) are part of a cycle to pump the value of `NFT B`.  

Digging into this we can see that there is a wallet cycle $A\rightarrow B\rightarrow C\rightarrow A$ over the course of 4 blocks, pumping the price from 0.8ETH to 2.6ETH.

![](https://www.raphtory.com/images/nfts/buy_nft.png)
*Example of an NFT Bipartite Graph. Red arrows indicate the transaction is part of a cycle, green arrows indicate the transaction is not.*

#### Why do we model the data like this? 

Cycle detection is a notoriously hard algorithm to run on large networks because you need to track the entire path you have traversed so far. This explodes exponentially with the size of the cycle you are looking for, quickly becoming impossible to calculate. This would be the case if we had to follow each NFT along the transaction edges in a network of wallets. 

By instead splitting the NFT's into their own set of nodes (as above) we have all the information on the transactions each has been involved in within the edge lists of the NFT vertex. With Raphtory's time-focused API's we can easily order all the edges chronologically and then loop through them to find all cycles. This means even if there are cycles involving thousands of transactions/wallets, they are just as easy to detect as triangles.


## Data

Data for this code can be found at the [OSF.IO here](https://osf.io/32zec/). 

The data consists of

1. `Data_API_reduced.csv` - A CSV file containing the Ethereum Transactions of NFT sales. Download and extract this file.

The data is pre-classified and contains transaction details for NFT sales including the buyers, sellers, type of NFT, price and time. The sales are spread across various tokens and cryptocurrencies, however, in some cases the price data in USD was unavailable. To handle this, we filtered the data to those traded with Ethereum where we could obtain the average $ Dollar \rightarrow Ether$ price for the day of the trade. This left us with 2.3 million trades across 3,384 NFTs, an example row of which can be seen below. 

```
| Smart_contract | ID_token | Transaction_hash | Seller_address | Seller_username | Buyer_address | Buyer_username | Price_Crypto | Crypto | Price_USD | Collection       | Market   | Datetime_updated_seconds | Collection_cleaned | Category    |
| 0xd73be..      | 66733    | 0xcdb2c..        | 0xd0c23..      | GorillaNixon    | 0xf6362..     | iMott          | 0.008        | ETH    | 15.4833   | Blockchaincuties | OpenSea  | 2021-04-01 00:05:09      | Block              | Collectible |
```


2. `ETH-USD.csv` - Price of Ethereum/USD pair per day
3. `results.json` - Final results after running the code 

The code should automatically download files `1` and `2`.


## Code Overview

There are two ways to run this example, via scala and python.

At present the scala code is highly optimised runs in less than 10 seconds.

### Scala

`src/main/scala/` contains the bulk of the code used in Raphtory

- `LocalRunner.scala` runs the example. This initialises and executes the components (FileSpout, GraphBuilder and Algorithm )
    - Runs a FileSpout (reads the Ethereum data from the above CSV file)
    - Executes the `cyclemania` algorithm
    - Saves the result to a `.json` file in the `/tmp/` folder
- `analysis/CycleMania.scala` the cycle detection algorithm

### Python

`src/main/python` contains
- `nft_analysis.ipynb` this is an interactive jupyter notebook running the same process as above,
  except all the code is written completely in Python. In addition, this notebook also explores the data
  and we find a numerous of wallets suspiciously trading NFTs between each other.
- `nft_helper.py` are some helper functions containing code to load our results, draw graphs and read the data file.

Please read the notebook for more information or follow the guide in the docs.

# Running this example

## Building the graph

### Imports 

Import all necessary dependencies needed to build a graph from your data in Raphtory. 


````{tabs}

```{code-tab} py
import time
from calendar import timegm
from pyraphtory.context import PyRaphtory
from pyraphtory.builder import *
from pyraphtory.spouts import FileSpout
from pyraphtory.sinks import FileSink
from pyraphtory.formats import JsonFormat
import csv
from nft_helper import *
```
```{code-tab} scala
import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.{EdgeList, NodeList}
import com.raphtory.api.input.Graph.assignID
import com.raphtory.api.input.{DoubleProperty, Graph, ImmutableProperty, Properties, Source, StringProperty, Type}
import com.raphtory.examples.nft.analysis.CycleMania
import com.raphtory.formats.JsonFormat
import com.raphtory.sinks.FileSink
import com.raphtory.utils.FileUtils
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Type

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import scala.io.Source
```
```` 

### Downloading the data 

````{tabs}

```{code-tab} py
!wget https://osf.io/download/mw3vh/ -O ETH-USD.csv
!wget https://osf.io/download/kaumt/ -O Data_API_reduced.csv 

```
```{code-tab} scala
val path = "/tmp/Data_API_reduced.csv"
val url  = "https://osf.io/download/kaumt/"
FileUtils.curlFile(path, url)

val eth_historic_csv = "/tmp/ETH-USD.csv"
val url_eth  = "https://osf.io/download/mw3vh/"
FileUtils.curlFile(eth_historic_csv, url_eth)
```
````

Output
```bash
--2022-09-29 15:23:42--  https://osf.io/download/mw3vh/
Resolving osf.io (osf.io)... 35.190.84.173
Connecting to osf.io (osf.io)|35.190.84.173|:443... connected.
HTTP request sent, awaiting response... 302 FOUND
Location: https://files.de-1.osf.io/v1/resources/32zec/providers/osfstorage/62daaec1f66a944cdb230877 [following]
--2022-09-29 15:23:43--  https://files.de-1.osf.io/v1/resources/32zec/providers/osfstorage/62daaec1f66a944cdb230877
Resolving files.de-1.osf.io (files.de-1.osf.io)... 35.186.249.111
Connecting to files.de-1.osf.io (files.de-1.osf.io)|35.186.249.111|:443... connected.
HTTP request sent, awaiting response... 302 Found
... 
Saving to: ‘ETH-USD.csv’

ETH-USD.csv         100%[===================>] 130.22K  --.-KB/s    in 0.03s   

2022-09-29 15:23:44 (3.82 MB/s) - ‘ETH-USD.csv’ saved [133343/133343]

--2022-09-29 15:23:44--  https://osf.io/download/kaumt/
Resolving osf.io (osf.io)... 35.190.84.173
Connecting to osf.io (osf.io)|35.190.84.173|:443... connected.
HTTP request sent, awaiting response... 302 FOUND
Location: https://files.de-1.osf.io/v1/resources/32zec/providers/osfstorage/6335669dec7f3f0278f5f1fe [following]
--2022-09-29 15:23:44--  https://files.de-1.osf.io/v1/resources/32zec/providers/osfstorage/6335669dec7f3f0278f5f1fe
Resolving files.de-1.osf.io (files.de-1.osf.io)... 35.186.249.111
Connecting to files.de-1.osf.io (files.de-1.osf.io)|35.186.249.111|:443... connected.
...
Saving to: ‘Data_API_reduced.csv’

Data_API_reduced.cs 100%[===================>] 109.81M  29.4MB/s    in 3.8s    

2022-09-29 15:23:49 (29.2 MB/s) - ‘Data_API_reduced.csv’ saved [115143857/115143857]
``` 

### Loading the price data

Now we load our price data into memory, so we can refer to Ethereum prices if any are missing. 

````{tabs}

```{code-tab} py
eth_price_csv = 'ETH-USD.csv'
filename = "Data_API_reduced.csv"
at_time = 1561661534

date_price = setup_date_prices(eth_price_csv)
```

```{code-tab} scala
  def setupDatePrices(eth_historic_csv: String): mutable.HashMap[String, Double] = {
    val src              = scala.io.Source.fromFile(eth_historic_csv)
    val date_price_map   = new mutable.HashMap[String, Double]()
    src.getLines.drop(1).foreach { line =>
      val l = line.split(",").toList
      date_price_map.put(l(0), (l(1).toDouble + l(2).toDouble) / 2)
    }
    src.close()
    date_price_map
  }

  var date_price = setupDatePrices(eth_historic_csv = eth_historic_csv)

```
````

### Creating a Raphtory Graph  📊

Next we initialise Raphtory by creating a new graph.

````{tabs}

```{code-tab} py
def parse_graph(graph, line):
    file_line = line.split(',')
    if file_line[0] == "Smart_contract":
        return
    # Seller details
    seller_address = file_line[3]
    seller_address_hash = graph.assign_id(seller_address)
    # Buyer details
    buyer_address = file_line[5]
    buyer_address_hash = graph.assign_id(buyer_address)
    # Transaction details
    datetime_str = file_line[13]
    timestamp_utc = time.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
    timestamp = timegm(timestamp_utc)
    tx_hash = file_line[2]
    token_id_str = file_line[1]
    token_id_hash = graph.assign_id(token_id_str)

    crypto = file_line[8]
    if crypto != 'ETH':
        return

    if file_line[9] == "":
        price_usd = date_price[datetime_str[0:10]]
    else:
        price_usd = float(file_line[9])

    # NFT Details
    collection_cleaned = file_line[14]
    market = file_line[11]
    category = file_line[15]

    #  add buyer node
    graph.add_vertex(
        timestamp,
        buyer_address_hash,
        Properties(ImmutableProperty("address", buyer_address)),
        Type("Wallet")
    )

    # Add node for NFT
    graph.add_vertex(
        timestamp,
        token_id_hash,
        Properties(
            ImmutableProperty("id", token_id_str),
            ImmutableProperty("collection", collection_cleaned),
            ImmutableProperty("category", category)
        ),
        Type("NFT")
    )

    # Creating a bipartite graph,
    # add edge between buyer and nft
    graph.add_edge(
        timestamp,
        buyer_address_hash,
        token_id_hash,
        Properties(
            StringProperty("transaction_hash", tx_hash),
            StringProperty("crypto", crypto),
            StringProperty("price_usd", str(price_usd)),
            StringProperty("market", market),
            StringProperty("token_id", token_id_str),
            StringProperty("buyer_address", buyer_address)
        ),
        Type("Purchase")
    )

graph = PyRaphtory.new_graph()

graph.load(Source(FileSpout(filename), GraphBuilder(parse_graph)))
```
```{code-tab} scala
  def addToGraph(graph: Graph, tuple: String): Unit = {
    val fileLine            = tuple.split(",").map(_.trim)
    // Skip Header
    if (fileLine(0) == "Smart_contract") return
    // Seller details
    val seller_address      = fileLine(3)
    val seller_address_hash = assignID(seller_address)
    // Buyer details
    val buyer_address       = fileLine(5)
    val buyer_address_hash  = assignID(buyer_address)
    // Transaction details
    val datetime_str        = fileLine(13)
    val timeStamp           = LocalDateTime
      .parse(datetime_str, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
      .toEpochSecond(ZoneOffset.UTC)
    val tx_hash             = fileLine(2)
    val token_id_str        = fileLine(1)
    val token_id_hash       = assignID(token_id_str)
    val crypto              = fileLine(8)
    if (crypto != "ETH")
      return
    var price_USD           = 0.0
    if (fileLine(9) == "")
      price_USD = date_price(datetime_str.substring(0, 10))
    else
      price_USD = fileLine(9).toDouble

    // NFT Details
    val collection_cleaned = fileLine(14)
    val market             = fileLine(11)
    val category           = fileLine(15)

    // add buyer node
    graph.addVertex(
      timeStamp,
      buyer_address_hash,
      Properties(ImmutableProperty("address", buyer_address)),
      Type("Wallet")
    )

    // Add node for NFT
    graph.addVertex(
      timeStamp,
      token_id_hash,
      Properties(
        ImmutableProperty("id", token_id_str),
        ImmutableProperty("collection", collection_cleaned),
        ImmutableProperty("category", category)
      ),
      Type("NFT")
    )

    // Creating a bipartite graph,
    // add edge between buyer and nft
    graph.addEdge(
      timeStamp,
      buyer_address_hash,
      token_id_hash,
      Properties(
        StringProperty("transaction_hash", tx_hash),
        StringProperty("crypto", crypto),
        DoubleProperty("price_USD", price_USD),
        StringProperty("market", market),
        StringProperty("token_id", token_id_str),
        StringProperty("buyer_address", buyer_address)
      ),
      Type("Purchase")
    )
  }

  val graph = Raphtory.newGraph()
  val file = scala.io.Source.fromFile(path)

  file.getLines.foreach { line => addToGraph(graph, line) }
```
````

Expected output 

```bash
15:24:44.548 [Thread-12] INFO  com.raphtory.internals.context.LocalContext$ - Creating Service for 'suspicious_purple_marmot'
15:24:44.555 [io-compute-blocker-1] ERROR com.raphtory.internals.management.Prometheus$ - Failed to start Prometheus on port 9999
15:24:44.555 [io-compute-blocker-1] INFO  com.raphtory.internals.management.Prometheus$ - Prometheus started on port /0:0:0:0:0:0:0:0:63451
15:24:44.576 [io-compute-blocker-1] INFO  com.raphtory.internals.components.partition.PartitionOrchestrator$ - Creating '1' Partition Managers for 'suspicious_purple_marmot'.
15:24:44.579 [io-compute-blocker-8] INFO  com.raphtory.internals.components.partition.PartitionManager - Partition 0: Starting partition manager for 'suspicious_purple_marmot'.
15:24:44.832 [spawner-akka.actor.default-dispatcher-12] INFO  com.raphtory.internals.components.ingestion.IngestionManager - Ingestion Manager for 'suspicious_purple_marmot' establishing new data source
15:24:44.844 [io-compute-5] INFO  com.raphtory.spouts.FileSpoutInstance - Spout: Processing file 'Data_API_reduced.csv' ...
15:24:44.854 [spawner-akka.actor.default-dispatcher-12] INFO  com.raphtory.internals.components.querymanager.QueryManager - Source '0' is blocking analysis for Graph 'suspicious_purple_marmot'
```

### Executing the cycle detection algorithm 

Now we execute our cycle detection algorithm. 

We will run the algorithm at the latest point in time, which we have to manually set. 

For each NFT our algorithm will: 

- For each time it was purchased, checks the list of purchases to see if a user re-bought the NFT at a later date, at a higher price

This all happens within the step function. A step is executed once on each node.

The code has been optimised to O(n) complexity.


````{tabs}

```{code-tab} py
from pyraphtory.algorithm import PyAlgorithm
from pyraphtory.graph import TemporalGraph, Row, Table
from pyraphtory.vertex import Vertex
import pyraphtory.scala.collection

CYCLES_FOUND: str = "CYCLES_FOUND"


class CycleMania(PyAlgorithm):
    def __call__(self, graph: TemporalGraph) -> TemporalGraph:
        def step(v: Vertex):
            if v.type() != "NFT":
                v[CYCLES_FOUND] = []
                return
            all_cycles = []
            all_purchases = sorted(v.explode_in_edges(), key=lambda e: e.timestamp())
            purchasers = list(map(lambda e:
                                  dict(price_usd=float(e.get_property_or_else("price_usd", 0.0)),
                                       nft_id=e.get_property_or_else("token_id", "_UNKNOWN_"),
                                       tx_hash=e.get_property_or_else("transaction_hash", ""),
                                       time=e.timestamp(),
                                       buyer=e.get_property_or_else("buyer_address", "_UNKNOWN_")),
                                  all_purchases))
            if len(purchasers) > 2:
                buyers_seen = {}
                for pos, item_sale in enumerate(purchasers):
                    buyer_id = item_sale['buyer']
                    if buyer_id not in buyers_seen:
                        buyers_seen[buyer_id] = pos
                    else:
                        prev_pos = buyers_seen[buyer_id]
                        prev_price = purchasers[prev_pos]['price_usd']
                        current_price = item_sale['price_usd']
                        buyers_seen[buyer_id] = pos
                        if prev_price < current_price:
                            all_cycles.append(purchasers[prev_pos:pos + 1])
            if len(all_cycles):
                v[CYCLES_FOUND] = all_cycles
            else:
                v[CYCLES_FOUND] = []

        return graph.reduced_view().step(step)

    def tabularise(self, graph: TemporalGraph):
        def get_cycles(v: Vertex):
            vertex_type = v.type()
            rows_found = [Row()]
            if vertex_type == "NFT" and len(v[CYCLES_FOUND]):
                nft_id = v.get_property_or_else('id', '_UNKNOWN_')
                cycles_found = v[CYCLES_FOUND]
                nft_collection = v.get_property_or_else('collection', '_UNKNOWN_')
                nft_category = v.get_property_or_else('category', '_UNKNOWN_')
                rows_found = list(map(lambda single_cycle:
                                      Row(
                                          nft_id,
                                          nft_collection,
                                          nft_category,
                                          len(single_cycle),
                                          dict(cycle={'sales': single_cycle},
                                               profit_usd=float(single_cycle[len(single_cycle) - 1]['price_usd']) -
                                                          float(single_cycle[0]['price_usd']),
                                               buyer=str(single_cycle[0]['buyer']))
                                      ), cycles_found))
            return rows_found

        return graph.explode_select(lambda v: get_cycles(v)).filter(lambda row: len(row.get_values()) > 0)

graph \
    .at(at_time) \
    .past() \
    .execute(CycleMania()) \
    .write_to(FileSink('/tmp/raphtory_nft_python', format=JsonFormat())) \
    .wait_for_job()
```
```{code-tab} scala

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.algorithm.GenericReduction
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

import scala.collection.mutable
// lets get ready to rumble

class CycleMania(moneyCycles: Boolean = true) extends Generic {

  // helper function to print cycle information
  def printCycleInfo(purchasers: List[Any], i: Int, j: Int): Unit = {
    print("Found an NFT cycle that sold for profit : ")
    for (k <- i to j)
      print(" " + purchasers(k))
    println()
  }

  final val HAS_CYCLE: String    = "HAS_CYCLE"
  final val CYCLES_FOUND: String = "CYCLES_FOUND"

  override def apply(graph: GraphPerspective): graph.Graph =
    graph.reducedView
      .step { vertex =>
        // only for vertexes that are of type NFT
        if (vertex.Type == "NFT") {
          var allCyclesFound: List[Cycle] = List()
          // get all of my incoming exploded edges and sort them by time
          val allPurchases                = vertex.explodeInEdges().sortBy(e => e.timestamp)
          // get all the buyers
          // SRC = seller, DST = NFT they bought, Price_USD = Price,
          val purchasers                  = allPurchases.map(e =>
            Sale(
                    e.getPropertyOrElse("buyer_address", "_UNKNOWN_"),
                    e.getPropertyOrElse("price_USD", 0.0),
                    e.timestamp,
                    e.getPropertyOrElse("transaction_hash", ""),
                    e.getPropertyOrElse("token_id", "_UNKNOWN_")
            )
          )
          if (purchasers.size > 2) {
            // for each buyer, and when they bought it print this as a cycle
            // we keep track of buyers we have seen
            val buyersSeen = mutable.HashMap[String, Int]()
            for ((itemSale, position) <- purchasers.view.zipWithIndex) {
              // If we have not seen this buyer, then add it to the hashmap with the index it was seen
              val buyerId = itemSale.buyer
              if (!buyersSeen.contains(buyerId))
                buyersSeen.addOne((buyerId, position))
              // if we have seen this buyer, then it means we have a cycle omg
              // we print this as a cycle we have seen, then we update the position
              // as we do not want to double count the cycles
              else {
                // but only if the buyer has paid for the item more the second time
                val previousBuyerPosition = buyersSeen.getOrElse(buyerId, -1)
                val previousPrice         = purchasers(previousBuyerPosition).price_usd
                val currentPrice          = itemSale.price_usd
                if (moneyCycles) {
                  // ISSUE: If user is the same and at a loss, then the cycle keeps going.
                  buyersSeen.update(buyerId, position)
//                  println(f"prev $previousPrice : current: $currentPrice")
                  if (previousPrice < currentPrice)
                    // println(f"Money Cycle found, item $buyerId, from ${buyersSeen.get(buyerId)} to $position ")
                    allCyclesFound =
                      Cycle(purchasers.slice(previousBuyerPosition, position + 1).toArray[Sale]) :: allCyclesFound
                }
                else {
                  // println(f"All Cycle found, item $buyerId, from ${buyersSeen.get(buyerId)} to $position ")
                  buyersSeen.update(buyerId, position)
                  allCyclesFound =
                    Cycle(purchasers.slice(previousBuyerPosition, position + 1).toArray[Sale]) :: allCyclesFound
                }
              }
            }
          }
          if (allCyclesFound.nonEmpty) {
            vertex.setState(CYCLES_FOUND, allCyclesFound)
            vertex.setState(HAS_CYCLE, true)
          }
        }
      }
      .asInstanceOf[graph.Graph]

  override def tabularise(graph: GraphPerspective): Table =
    graph
      .explodeSelect { vertex =>
        val vertexType         = vertex.Type
        val has_cycle: Boolean = vertex.getStateOrElse(HAS_CYCLE, false)
        if (vertexType == "NFT" & has_cycle) {
          val nftID                    = vertex.getPropertyOrElse("id", "_UNKNOWN_")
          val cyclesFound: List[Cycle] = vertex.getState(CYCLES_FOUND)
          val nftCollection            = vertex.getPropertyOrElse("collection", "_UNKNOWN_")
          val nftCategory              = vertex.getPropertyOrElse("category", "_UNKNOWN_")
          cyclesFound.map { singleCycle =>
            val cycleData: CycleData = CycleData(
                    buyer = singleCycle.sales.head.buyer,
                    profit_usd = singleCycle.sales.last.price_usd - singleCycle.sales.head.price_usd,
                    cycle = singleCycle
            )
            Row(
                    nftID,
                    nftCollection,
                    nftCategory,
                    singleCycle.sales.length,
                    cycleData
            )
          }
        }
        else
          List(Row())
      }
      .filter(row => row.getValues().nonEmpty)

  case class Sale(buyer: String, price_usd: Double, time: Long, tx_hash: String, nft_id: String)

  case class Cycle(sales: Array[Sale])

  case class CycleData(buyer: String, profit_usd: Double, cycle: Cycle)

  case class Node(
      nft_id: String,
      nft_collection: String,
      nft_category: String,
      cycles_found: Int,
      cycle_data: CycleData
  )

}

object CycleMania {
  def apply() = new CycleMania()
}

val atTime = 1561661534
graph
    .at(atTime)
    .past()
    .execute(CycleMania())
    .writeTo(FileSink("/tmp/raphtory_nft_scala", format = JsonFormat()))
    .waitForJob()
```
````

Expected output

```bash
15:24:53.723 [io-compute-blocker-3] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job CycleMania_6248853746968651925: Starting query progress tracker.
15:24:53.738 [spawner-akka.actor.default-dispatcher-6] INFO  com.raphtory.internals.components.querymanager.QueryManager - Query 'CycleMania_6248853746968651925' currently blocked, waiting for ingestion to complete.
15:27:25.132 [spawner-akka.actor.default-dispatcher-7] INFO  com.raphtory.internals.components.querymanager.QueryManager - Source '0' is unblocking analysis for Graph 'suspicious_purple_marmot' with 1077657 messages sent. Latest update time was 1564271447
15:27:25.134 [spawner-akka.actor.default-dispatcher-7] INFO  com.raphtory.internals.components.querymanager.QueryManager - Source 0 currently ingested 99.0% of its updates.
15:27:25.198 [spawner-akka.actor.default-dispatcher-7] INFO  com.raphtory.internals.components.querymanager.QueryManager - Source 0 has completed ingesting and will now unblock
15:27:25.199 [spawner-akka.actor.default-dispatcher-7] INFO  com.raphtory.internals.components.querymanager.QueryManager - Query 'CycleMania_6248853746968651925' received, your job ID is 'CycleMania_6248853746968651925'.
15:27:25.217 [spawner-akka.actor.default-dispatcher-12] INFO  com.raphtory.internals.components.partition.QueryExecutor - CycleMania_6248853746968651925_0: Starting QueryExecutor.
17:01:06.660 [spawner-akka.actor.default-dispatcher-7] INFO  com.raphtory.internals.components.querymanager.QueryHandler - Job 'CycleMania_6248853746968651925': Perspective at Time '1561661534'. 
[spawner-akka.actor.default-dispatcher-6] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job 'CycleMania_6248853746968651925': Perspective '1561661534' finished.
[spawner-akka.actor.default-dispatcher-6] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job CycleMania_6248853746968651925: Running query, processed 1 perspectives.
[spawner-akka.actor.default-dispatcher-7] INFO  com.raphtory.api.querytracker.QueryProgressTracker - Job CycleMania_6248853746968651925: Query completed with 1 perspectives and finished.
```

## Analysing the graph







