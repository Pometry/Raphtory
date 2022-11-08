# What's Awash With NFTs: Finding Suspicious Trading Cycles

## Overview

In this example we will use Raphtory to find users that have been trading NFTs in cycles. At the end of this example, you will have been able to: 
- Read a CSV into a Raphtory bipartite graph
- Run a cycle detection algorithm
- Analyse the results to find a list of suspicious NFT traders

The example is presented in both in Scala and Python.
This is the code used to obtain the insights from the Raphtory [NFT blogpost](raphtory.com/nfts/) .

## Pre-requisites

Follow our Installation guide: [Install](../Install/start.md)

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

## Running this example

### Building the graph

#### Imports 

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

#### Downloading the data 

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

#### Loading the price data

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

#### Creating a Raphtory Graph  📊

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

### Analysing the graph

First we import the tools we are using for analysis


```python
import pandas as pd
import json
import seaborn as sns
from scipy import stats
import numpy as np
import time
import matplotlib.pyplot as plt
plt.rcParams['font.size'] = '16'
from nft_helper import *
from collections import Counter
```

#### Reading the results

We will now read the data that Raphtory produced. 

Please check which path the data was saved too, and adjust the line below. 

In our case, Raphtory saved the data to the `CycleMania_8220652547042296969` folder. 


```python
# data = load_json('/tmp/raphtory_nft_python/CycleMania_8220652547042296969/partition-0.json')
data = load_json("/private/tmp/raphtory_nft_python/CycleMania_1704408906529405662/partition-0.json")
new_data = []
# filter any cycles that are less than 2 hops
for d in data:
    if d['row'][3] > 2:
        new_data.append(d)
data = new_data
data_df = pd.DataFrame(data)
```

    amount of data 11848



```python
data_df["profit"]=data_df["row"].apply(lambda x: x[4]['profit_usd'])
data_df["min_ts"] = data_df["row"].apply(lambda x: min(map(lambda y: y["time"],x[4]['cycle']['sales'])))
data_df["max_ts"] = data_df["row"].apply(lambda x: max(map(lambda y: y["time"],x[4]['cycle']['sales'])))
data_df["length"] = data_df["row"].apply(lambda x: len(x[4]['cycle']['sales']))
data_df["duration_days"] = (data_df["max_ts"] - data_df["min_ts"])/86400
display(data_df)
```

Expected output

<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>timestamp</th>
      <th>window</th>
      <th>row</th>
      <th>profit</th>
      <th>min_ts</th>
      <th>max_ts</th>
      <th>length</th>
      <th>duration_days</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1561661534</td>
      <td>None</td>
      <td>[34508_Etheremon, Etheremon, Games, 3, {'cycle...</td>
      <td>0.288145</td>
      <td>1549997538</td>
      <td>1550054540</td>
      <td>3</td>
      <td>0.659745</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1561661534</td>
      <td>None</td>
      <td>[34508_Etheremon, Etheremon, Games, 3, {'cycle...</td>
      <td>0.288145</td>
      <td>1550011944</td>
      <td>1550068941</td>
      <td>3</td>
      <td>0.659687</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1561661534</td>
      <td>None</td>
      <td>[34508_Etheremon, Etheremon, Games, 17, {'cycl...</td>
      <td>0.038025</td>
      <td>1549888079</td>
      <td>1550197039</td>
      <td>17</td>
      <td>3.575926</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1561661534</td>
      <td>None</td>
      <td>[34508_Etheremon, Etheremon, Games, 4, {'cycle...</td>
      <td>0.172380</td>
      <td>1550254166</td>
      <td>1550339637</td>
      <td>4</td>
      <td>0.989248</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1561661534</td>
      <td>None</td>
      <td>[34508_Etheremon, Etheremon, Games, 12, {'cycl...</td>
      <td>0.063375</td>
      <td>1550154555</td>
      <td>1550354072</td>
      <td>12</td>
      <td>2.309225</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>11698</th>
      <td>1561661534</td>
      <td>None</td>
      <td>[1043670_Cryptokittie, Cryptokittie, Art, 24, ...</td>
      <td>4.619700</td>
      <td>1561185101</td>
      <td>1561527116</td>
      <td>24</td>
      <td>3.958507</td>
    </tr>
    <tr>
      <th>11699</th>
      <td>1561661534</td>
      <td>None</td>
      <td>[1043670_Cryptokittie, Cryptokittie, Art, 19, ...</td>
      <td>3.958875</td>
      <td>1561270650</td>
      <td>1561541217</td>
      <td>19</td>
      <td>3.131562</td>
    </tr>
    <tr>
      <th>11700</th>
      <td>1561661534</td>
      <td>None</td>
      <td>[1043670_Cryptokittie, Cryptokittie, Art, 15, ...</td>
      <td>3.968325</td>
      <td>1561341719</td>
      <td>1561555618</td>
      <td>15</td>
      <td>2.475683</td>
    </tr>
    <tr>
      <th>11701</th>
      <td>1561661534</td>
      <td>None</td>
      <td>[1043670_Cryptokittie, Cryptokittie, Art, 23, ...</td>
      <td>4.619700</td>
      <td>1561242169</td>
      <td>1561569720</td>
      <td>23</td>
      <td>3.791100</td>
    </tr>
    <tr>
      <th>11702</th>
      <td>1561661534</td>
      <td>None</td>
      <td>[1043670_Cryptokittie, Cryptokittie, Art, 9, {...</td>
      <td>2.471850</td>
      <td>1561470256</td>
      <td>1561584330</td>
      <td>9</td>
      <td>1.320301</td>
    </tr>
  </tbody>
</table>
<p>11703 rows × 8 columns</p>
</div>


#### Cycle length distributions

Below is a simple CDF showing the distribution of the lengths of cycles. 


```python
x,y = ccdf(data_df.duration_days)

fig, ax = plt.subplots()
ax.set_xscale("log")
ax.set_yscale("log")
ax.plot(x,y, marker="o")
ax.set_ylabel("CCDF")
ax.set_xlabel("Cycle duration (days)")

plt.show()
```

Expected output

 <img src="../_static/NFT_output_27_0.png" width="700px" style="padding: 15px"/>


```python
x,y = ccdf(data_df.length)

fig, ax = plt.subplots()
ax.set_xscale("log")
ax.set_yscale("log")
ax.plot(x,y, marker="o")
ax.set_ylabel("CCDF")
ax.set_xlabel("Cycle length (hops)")

plt.show()
```

Expected output

<img src="../_static/NFT_output_28_0.png" width="700px" style="padding: 15px"/>


#### How many traders took part in cycles?


```python
traders_profit = {}
for row in data:
    trade = {
        'profit_usd': row['row'][4]['profit_usd'],
        'buyer':  row['row'][4]['buyer'],
        'cycle_size': row['row'][3],
        'nft_id': row['row'][0],
        'cycle': row['row'][4]['cycle']['sales']
    }
    trader = trade['buyer']
    profit = trade['profit_usd']
    if trader not in traders_profit:
        traders_profit[trader] = []
    traders_profit[trader].append(profit)
traders_avg = {}
for trader, _profit in traders_profit.items():
    avg = sum(_profit)/len(_profit)
    traders_avg[trader] = {
        'avg': avg, 
        'count': len(_profit),
        'total_profit': sum(_profit)
    }
    
traders_count_sorted = sorted(traders_avg, key=lambda x: traders_avg[x]['count'], reverse=True)

for t in traders_count_sorted[:50]:
    print(t)
    print(traders_avg[t])
    print()
```

    0x8acc1421ec98689461ff5777de8ad6648dc6d643
    {'avg': 2.251955391740451, 'count': 617, 'total_profit': 1389.4564767038582}
    
    0x00c9da65b33b4f7034b5f50b90f5f6d8320d6ab8
    {'avg': 2.1960905438311666, 'count': 616, 'total_profit': 1352.7917749999986}
    
    0xa21e0974137bf8400eb7dca606d9dcb190d79ed9
    {'avg': 2.3006426628664496, 'count': 614, 'total_profit': 1412.594595}
    
    0x838c14eb3eabe4cb6a696d286c7b2a466629d0ee
    {'avg': 2.357247238562093, 'count': 612, 'total_profit': 1442.6353100000008}
    
    0x1c1ef71445010114f41ac1feb32dbf5d7281e90f
    {'avg': 2.3474603371710527, 'count': 608, 'total_profit': 1427.255885}
    
    0xe738725cdcc41c91f734dd7b5b9659df994d6dda
    {'avg': 2.2413728476821193, 'count': 604, 'total_profit': 1353.7892000000002}
    
    0x6a47b60c377450a460d3eb828d534ee66eead668
    {'avg': 2.2968711341059596, 'count': 604, 'total_profit': 1387.3101649999996}
    
    0x463215edb66fb6a8f0c979e739a731977617699f
    {'avg': 2.170596011705686, 'count': 598, 'total_profit': 1298.0164150000003}
    
    0x179d698f5a1c84c3ff4c5eb04e553c15a0c1d8d8
    {'avg': 2.2783097403685084, 'count': 597, 'total_profit': 1360.1509149999995}
    
    0x1e7f320cf5a938465d501f6bd6c405feb3a70f6c
    {'avg': 2.2265507202680097, 'count': 597, 'total_profit': 1329.2507800000017}
    
    0x6e13c7e25c2cda6f5c8c4e431bee480bfb312c28
    {'avg': 2.2607215195246195, 'count': 589, 'total_profit': 1331.564975000001}
    
    0x236ef21dc36d0aec3990dd5ee84a9d5320644262
    {'avg': 2.302746853448277, 'count': 580, 'total_profit': 1335.5931750000007}
    
    0xf5aee6d7b838d5ede8aa65d31dbc11116545180c
    {'avg': 2.279057060869566, 'count': 575, 'total_profit': 1310.4578100000006}
    
    0xbabda06088c242fb2a763aa7cc99706cb77ba735
    {'avg': 2.3128609598603824, 'count': 573, 'total_profit': 1325.2693299999992}
    
    0x68d31cb3825e559b1e5c0665f2d65d06a17fce1a
    {'avg': 2.1250271578947335, 'count': 570, 'total_profit': 1211.2654799999982}
    
    0xa37e6b46fa8e1a6f1ddbf035c4e0230b8414ff04
    {'avg': 2.1527055114638443, 'count': 567, 'total_profit': 1220.5840249999997}
    
    0xcdcadf0279ee021a0c40a31ac10fa69e028e21d0
    {'avg': 2.305673079710142, 'count': 552, 'total_profit': 1272.7315399999984}
    
    0x7316e9cf94bef40d4981d66a5c41c38b6b32454c
    {'avg': 2.2200820802919696, 'count': 548, 'total_profit': 1216.6049799999994}
    
    0x87b77fabfeb869150b8e1b9462603f9a639c5fae
    {'avg': 2.285133267419962, 'count': 531, 'total_profit': 1213.4057649999997}
    
    0xadd12bd6375dc21d579cc4abcfa04864d6ac9a62
    {'avg': 2.2776891950757596, 'count': 528, 'total_profit': 1202.6198950000012}
    
    0xf6d22b698d04634f218fc03788784fa55765c7b4
    {'avg': 811.62565, 'count': 2, 'total_profit': 1623.2513}
    
    0xc160ce5a535772b3c606a46e19008578e277d55c
    {'avg': 2.0031915056792546, 'count': 2, 'total_profit': 4.006383011358509}
    
    0x44918a232e860498a54a74f8c312e4a6a4b0ccf9
    {'avg': 1.7402699999999998, 'count': 2, 'total_profit': 3.4805399999999995}
    
    0x36b6e5010732bf1cd0d5178fd130a11635dc40e3
    {'avg': 1.9479800000000012, 'count': 1, 'total_profit': 1.9479800000000012}
    
    0x056bfac0c2abca95490027a6e29e9bf039f1d0f6
    {'avg': 3.415020000000002, 'count': 1, 'total_profit': 3.415020000000002}
    
    0xfd380c3c8c079e1161bb0f95772e91875b76f80a
    {'avg': 16.213850000000004, 'count': 1, 'total_profit': 16.213850000000004}
    
    0x0945255148a4e0fafb31af7964d67a827442c8fd
    {'avg': 240.57750000000004, 'count': 1, 'total_profit': 240.57750000000004}
    
    0x7c259c9162056e109ea5ffdb1a1b6fefdcd4b907
    {'avg': 4.645528535639883, 'count': 1, 'total_profit': 4.645528535639883}
    
    0xfd5aa534af56244a1d7b1740e7783ae7479dfc3d
    {'avg': 0.005355009970895497, 'count': 1, 'total_profit': 0.005355009970895497}
    
    0xee68dc68913ee5606608c90d9b5db8ec4b12774c
    {'avg': 0.7395807262827934, 'count': 1, 'total_profit': 0.7395807262827934}
    
    0x8805c680b191a244fb41a8d475e6fe33031396e4
    {'avg': 17.599675646873358, 'count': 1, 'total_profit': 17.599675646873358}
    
    0x0239769a1adf4def9f07da824b80b9c4fcb59593
    {'avg': 8.00545, 'count': 1, 'total_profit': 8.00545}
    
    0x6be4a7bbb812bfa6a63126ee7b76c8a13529bdb8
    {'avg': 8.524000000000004, 'count': 1, 'total_profit': 8.524000000000004}
    
    0x530cf036ed4fa58f7301a9c788c9806624cefd19
    {'avg': 0.027382013131371688, 'count': 1, 'total_profit': 0.027382013131371688}
    
    0xd94d7922ed9dfa3bdb4253773bcdc78d5fc162e6
    {'avg': 19.512317006868845, 'count': 1, 'total_profit': 19.512317006868845}
    
    0x37c701ecd86cdddba614cf194d9a41d1f9c55145
    {'avg': 83.85087499999995, 'count': 1, 'total_profit': 83.85087499999995}
    
    0x7e1dcf785f0353bf657c38ab7865c1f184efe208
    {'avg': 182.58500000000004, 'count': 1, 'total_profit': 182.58500000000004}
    
    0x6b924183307ef2e529db662f80200611328a0cbd
    {'avg': 44.4174, 'count': 1, 'total_profit': 44.4174}
    
    0x56ec15bd7268d71154809dfc5042381168139502
    {'avg': 0.010728176782828835, 'count': 1, 'total_profit': 0.010728176782828835}
    
    0xa1a4ad46867ff5b4cebd83a2a88c2de5637ef2e7
    {'avg': 19.863466581809753, 'count': 1, 'total_profit': 19.863466581809753}
    


Above, we can see that 20 traders participated in over 528 cycles each. 

Lets write some code to inspect these NFT cycles in more depth. 

The `pretty_cycle` function takes a cycle and prints out each subsequent trade and the profit and time between trades.  


```python
def pretty_cycle(cycle):
    as_string = '   '
    prev_price = cycle[0]['price_usd']
    prev_time = cycle[0]['time']
    for item in cycle:
        diff = item['price_usd']-prev_price
        time_secs = item['time']-prev_time
        time_mins = time_secs/60
        time_hours = time_secs/60/60
        time_days = time_secs/60/60/24 
        prev_time = item['time']
        time_str = '%.1fm/%.1fh/%.2fd' % (time_mins, time_hours, time_days)
        as_string += 'T(d) '+time_str+', B: '+item['buyer'][:4]+'.. $'+str(item['price_usd'])+'('+str(diff)+') '+item['tx_hash']+'\n->'
    as_string = as_string[:-3]
    print(as_string)
```

#### Lets take a look at the trader who did the most cycles


```python
largest_trader_addr = traders_count_sorted[0] # '0x8acc1421ec98689461ff5777de8ad6648dc6d643'
largest = []
for trade in data:
    # ignore any rari tokens, as they are fractionalised
    #. ignore any short trades 
    if '_Rari' in trade['row'][0] or trade['row'][3] <= 2:
        continue
    if trade['row'][4]['buyer'] == largest_trader_addr:
        largest.append(trade)
```

##### Which NFTs did they trade?


```python
from collections import Counter
most_common_nft_collection = []
most_common_nft = []
for trade in largest:
    nft_collection =  trade['row'][0].split('_')
    if len(nft_collection) > 2:
        print("crap")
    most_common_nft.append(trade['row'][0])
    most_common_nft_collection.append(nft_collection[1])
most_common_nft_collection = set(most_common_nft_collection)
most_common_nft_collection = Counter(most_common_nft_collection)
most_common_nft = Counter(most_common_nft)
most_common_nft_collection
```


Expected output

    Counter({'Cryptokittie': 1, 'Mlbchampion': 1, 'Etheremon': 1})



##### What happens when we look at their cycles in more detail?


```python
pretty_cycle(largest[10]['row'][4]['cycle']['sales'])
```

Expected output

       T(d) 0.0m/0.0h/0.00d, B: 0x8a.. $28.07597(0.0) 0xa24a0a9d40be11656eda50dc51b19203a471ad4ee30d5337fd2341bc1aab1eeb
    ->T(d) 241.0m/4.0h/0.17d, B: 0x1c.. $28.07597(0.0) 0x35be9b454cb00f50889573b67d6c2cf8548f9aa1eda0aab92de93231f04e6b91
    ->T(d) 233.9m/3.9h/0.16d, B: 0x6a.. $28.07597(0.0) 0x4e06d7a08b1e9375907f428a31841c8918f51cce3bac9caff99c0adbe0e61aa2
    ->T(d) 241.5m/4.0h/0.17d, B: 0x1c.. $28.07597(0.0) 0x73de7764f6a2491ba017a5e8828dad65f0f3bc6a30dacbffffc386401a5aaac4
    ->T(d) 233.6m/3.9h/0.16d, B: 0x23.. $28.07597(0.0) 0xe329179aa4ac5447ef8781e3fab896e9ee349bf47d7021e72952dfbfe47ef04a
    ->T(d) 1190.0m/19.8h/0.83d, B: 0x1c.. $29.222635000000004(1.1466650000000023) 0xef40935f696595a917704fda9066214f79545c55dba3aa873684e5d1f5781fe2
    ->T(d) 235.2m/3.9h/0.16d, B: 0x23.. $29.222635000000004(1.1466650000000023) 0xa10a3d9369629cf63291d661008da58996a9d79be4bbac5ea95775f682ea5eba
    ->T(d) 240.4m/4.0h/0.17d, B: 0x1c.. $30.80025(2.7242799999999967) 0xd0e2263a79ff59a1f51845187de8693441bf1fec254372e0c8a3b2512c78418c
    ->T(d) 236.2m/3.9h/0.16d, B: 0xa3.. $30.80025(2.7242799999999967) 0xb3a0fc0b39cef5da2ed82161fd4173fb359626acd627e5d051990396f5567096
    ->T(d) 717.3m/12.0h/0.50d, B: 0x00.. $30.80025(2.7242799999999967) 0x79bedde202586c2b84c99ca0afea548f2539b2aa7866a34f6bbdf1c1493ce059
    ->T(d) 232.5m/3.9h/0.16d, B: 0xa3.. $30.80025(2.7242799999999967) 0x8933366def5646c8228da557fe243ad28f8eb9d6a262be5c59b62bb76a1f2efe
    ->T(d) 239.1m/4.0h/0.17d, B: 0xf5.. $29.961165000000005(1.885195000000003) 0x532cff8cedd3158db4a0175904df67226b659012f05f8f05e504a5192b343ae7
    ->T(d) 710.0m/11.8h/0.49d, B: 0xad.. $29.961165000000005(1.885195000000003) 0xc30f60cc1ca3715e01cabb4cd496326044b46457906cf86ec4c52b82be354d93
    ->T(d) 239.3m/4.0h/0.17d, B: 0xf5.. $29.961165000000005(1.885195000000003) 0xc2b2e7cf1b86537fc1307b0c67563f90e82300d8ae12080c12ef121077c8c075
    ->T(d) 235.1m/3.9h/0.16d, B: 0x6e.. $29.961165000000005(1.885195000000003) 0x211eb8ff65c9c5c7dfee6f133e00df95c1531ba9abc5765fcd4f384ca516fd5f
    ->T(d) 240.2m/4.0h/0.17d, B: 0x17.. $30.358315(2.2823449999999994) 0x8dbff6a011fac73a0136e1dfe9f00592151dc7e750e26d96beaae6c4295336fa
    ->T(d) 235.1m/3.9h/0.16d, B: 0x87.. $30.358315(2.2823449999999994) 0xd39f546f67a7669a08867f82c6408c3cbcf2a35be80b88d5ad490de0bfaf6a8c
    ->T(d) 240.4m/4.0h/0.17d, B: 0x23.. $30.358315(2.2823449999999994) 0x092153fb53aa8ac5f918e4c41b2583d2daf2a405de8cb29d56bd268fff0c8d44
    ->T(d) 252.7m/4.2h/0.18d, B: 0x8a.. $30.358315(2.2823449999999994) 0x31866d326866ef7e71818ad8896d85b43be0d81169460a9c71f8f988630770a8


We have found something here, this trader trades each nft between wallets within a ~4 hour window??

##### What was the time between each transaction?


```python
import math
def time_finder(cycle):
    prev_time = cycle[0]['time']
    times = []
    for sale in cycle:
        time_secs = sale['time']-prev_time
        time_hours = time_secs/60/60
        prev_time = sale['time']
        time_hours = round(time_hours, 1)
        times.append(time_hours)
    return times

time_found = []
for trade in largest:
    time_found.extend(time_finder(trade['row'][4]['cycle']['sales']))
time_found = Counter(time_found)
print("total times : %i" % sum(time_found.values()))
between_39_41 = time_found[3.4]+time_found[3.5]+time_found[3.6] +time_found[3.7] +time_found[3.8] +time_found[3.9] +time_found[4.0] +time_found[4.1] +time_found[4.2]+time_found[4.3]+time_found[4.4]+time_found[4.5]+time_found[4.6]
print("times between 3.4 and 4.6 : %i - %.2fp" % (between_39_41, between_39_41/sum(time_found.values())*100))
```

Expected output

    total times : 13193
    times between 3.4 and 4.6 : 11658 - 88.37p


##### Who did they trade with?


```python
wallet_interactions = []
for trade in largest:
    for sale in trade['row'][4]['cycle']['sales']:
        wallet_interactions.append(sale['buyer'])
wallet_interactions = Counter(wallet_interactions)
sorted(wallet_interactions.items(), key=lambda x: x[1], reverse=True), len(wallet_interactions)
```

Expected output

    ([('0x8acc1421ec98689461ff5777de8ad6648dc6d643', 1234),
      ('0xa21e0974137bf8400eb7dca606d9dcb190d79ed9', 710),
      ('0x179d698f5a1c84c3ff4c5eb04e553c15a0c1d8d8', 674),
      ('0xe738725cdcc41c91f734dd7b5b9659df994d6dda', 662),
      ('0x1c1ef71445010114f41ac1feb32dbf5d7281e90f', 659),
      ('0x838c14eb3eabe4cb6a696d286c7b2a466629d0ee', 653),
      ('0xf5aee6d7b838d5ede8aa65d31dbc11116545180c', 649),
      ('0x6a47b60c377450a460d3eb828d534ee66eead668', 649),
      ('0xbabda06088c242fb2a763aa7cc99706cb77ba735', 644),
      ('0x00c9da65b33b4f7034b5f50b90f5f6d8320d6ab8', 635),
      ('0x463215edb66fb6a8f0c979e739a731977617699f', 624),
      ('0x1e7f320cf5a938465d501f6bd6c405feb3a70f6c', 623),
      ('0xa37e6b46fa8e1a6f1ddbf035c4e0230b8414ff04', 618),
      ('0x87b77fabfeb869150b8e1b9462603f9a639c5fae', 612),
      ('0x6e13c7e25c2cda6f5c8c4e431bee480bfb312c28', 600),
      ('0x236ef21dc36d0aec3990dd5ee84a9d5320644262', 596),
      ('0xadd12bd6375dc21d579cc4abcfa04864d6ac9a62', 594),
      ('0x7316e9cf94bef40d4981d66a5c41c38b6b32454c', 590),
      ('0x68d31cb3825e559b1e5c0665f2d65d06a17fce1a', 586),
      ('0xcdcadf0279ee021a0c40a31ac10fa69e028e21d0', 581)],
     20)



Interestingly they only traded their NFTs with 20 other wallets. 


```python
fellow_traders = list(wallet_interactions.keys())
for t in fellow_traders:
    print(t)
    print(traders_avg[t])
    print()
```

Expected output

    0x8acc1421ec98689461ff5777de8ad6648dc6d643
    {'avg': 2.251955391740451, 'count': 617, 'total_profit': 1389.4564767038582}
    
    0xadd12bd6375dc21d579cc4abcfa04864d6ac9a62
    {'avg': 2.2776891950757596, 'count': 528, 'total_profit': 1202.6198950000012}
    
    0x00c9da65b33b4f7034b5f50b90f5f6d8320d6ab8
    {'avg': 2.1960905438311666, 'count': 616, 'total_profit': 1352.7917749999986}
    
    0xf5aee6d7b838d5ede8aa65d31dbc11116545180c
    {'avg': 2.279057060869566, 'count': 575, 'total_profit': 1310.4578100000006}
    
    0xbabda06088c242fb2a763aa7cc99706cb77ba735
    {'avg': 2.3128609598603824, 'count': 573, 'total_profit': 1325.2693299999992}
    
    0x7316e9cf94bef40d4981d66a5c41c38b6b32454c
    {'avg': 2.2200820802919696, 'count': 548, 'total_profit': 1216.6049799999994}
    
    0x179d698f5a1c84c3ff4c5eb04e553c15a0c1d8d8
    {'avg': 2.2783097403685084, 'count': 597, 'total_profit': 1360.1509149999995}
    
    0xa37e6b46fa8e1a6f1ddbf035c4e0230b8414ff04
    {'avg': 2.1527055114638443, 'count': 567, 'total_profit': 1220.5840249999997}
    
    0x236ef21dc36d0aec3990dd5ee84a9d5320644262
    {'avg': 2.302746853448277, 'count': 580, 'total_profit': 1335.5931750000007}
    
    0x838c14eb3eabe4cb6a696d286c7b2a466629d0ee
    {'avg': 2.357247238562093, 'count': 612, 'total_profit': 1442.6353100000008}
    
    0xcdcadf0279ee021a0c40a31ac10fa69e028e21d0
    {'avg': 2.305673079710142, 'count': 552, 'total_profit': 1272.7315399999984}
    
    0x6a47b60c377450a460d3eb828d534ee66eead668
    {'avg': 2.2968711341059596, 'count': 604, 'total_profit': 1387.3101649999996}
    
    0x1e7f320cf5a938465d501f6bd6c405feb3a70f6c
    {'avg': 2.2265507202680097, 'count': 597, 'total_profit': 1329.2507800000017}
    
    0xe738725cdcc41c91f734dd7b5b9659df994d6dda
    {'avg': 2.2413728476821193, 'count': 604, 'total_profit': 1353.7892000000002}
    
    0xa21e0974137bf8400eb7dca606d9dcb190d79ed9
    {'avg': 2.3006426628664496, 'count': 614, 'total_profit': 1412.594595}
    
    0x87b77fabfeb869150b8e1b9462603f9a639c5fae
    {'avg': 2.285133267419962, 'count': 531, 'total_profit': 1213.4057649999997}
    
    0x463215edb66fb6a8f0c979e739a731977617699f
    {'avg': 2.170596011705686, 'count': 598, 'total_profit': 1298.0164150000003}
    
    0x68d31cb3825e559b1e5c0665f2d65d06a17fce1a
    {'avg': 2.1250271578947335, 'count': 570, 'total_profit': 1211.2654799999982}
    
    0x1c1ef71445010114f41ac1feb32dbf5d7281e90f
    {'avg': 2.3474603371710527, 'count': 608, 'total_profit': 1427.255885}
    
    0x6e13c7e25c2cda6f5c8c4e431bee480bfb312c28
    {'avg': 2.2607215195246195, 'count': 589, 'total_profit': 1331.564975000001}
    


All of these traders also participated in many cycles. 

##### What were the patterns of the fellow traders cycles?


```python
import random

fellow_traders
print(len(fellow_traders))

others_sibs = {}
for sibling in fellow_traders:
    others_sibs[sibling] = []
    for trade in data:
        if '_Rari' in trade['row'][0] or trade['row'][3] <= 2:
            continue
        if trade['row'][4]['buyer'] == sibling:
            others_sibs[sibling].append(trade)
    print(sibling)
    rand = random.choice(range(0,len(others_sibs[sibling])))
    pretty_cycle(others_sibs[sibling][rand]['row'][4]['cycle']['sales'])
    print()
```

Expected output

    20
    0x8acc1421ec98689461ff5777de8ad6648dc6d643
       T(d) 0.0m/0.0h/0.00d, B: 0x8a.. $41.43778500000001(0.0) 0xc5e9bcc47fd0f65af6235ed50eee8c5aa518e0893109ff37cc5bd539c7b37fb7
    ->T(d) 240.4m/4.0h/0.17d, B: 0x6a.. $41.43778500000001(0.0) 0xc1a99b2f9ac21bf92bf4abe66eada85a636d0dfdcb6cbf5a522000b0126f146d
    ->T(d) 235.2m/3.9h/0.16d, B: 0x23.. $26.07660000000001(-15.361185000000003) 0x65aeb2ba711ca31d268cbb064010d3a4b14c27c4b67be976d3c612e141a547e1
    ->T(d) 259.4m/4.3h/0.18d, B: 0x6a.. $41.8806(0.44281499999998886) 0xb7092e8eb9069a4f90ac9f180cc7f242c3e76fcf6db88c2f7c2046a3e489dba4
    ->T(d) 215.5m/3.6h/0.15d, B: 0x23.. $41.8806(0.44281499999998886) 0x265731d67906a6f3409e8fc87b3bb28240acd9525e83dd36c21956a34a2ac4b2
    ->T(d) 240.7m/4.0h/0.17d, B: 0x46.. $26.07660000000001(-15.361185000000003) 0xdad56dc7003134186558820164065ba1000d62c5d6b8c9ad2084d25244f0241a
    ->T(d) 233.7m/3.9h/0.16d, B: 0xcd.. $41.8806(0.44281499999998886) 0x761fee613c9b8f4c1d709fd1397a3a411603539041faf3b57561eb4e6e794c75
    ->T(d) 240.2m/4.0h/0.17d, B: 0x8a.. $41.8806(0.44281499999998886) 0xff4561e2ca7f70499a75c25ab947a341669005eaf14f94618b5b588610dc55c9
    
    0xadd12bd6375dc21d579cc4abcfa04864d6ac9a62
       T(d) 0.0m/0.0h/0.00d, B: 0xad.. $3.3839(0.0) 0xfe12e1d49ab29143d954e5aafae3dbcc90b5df00e20291f77c4ab83eef860f5b
    ->T(d) 234.9m/3.9h/0.16d, B: 0x8a.. $3.3839(0.0) 0x07d3e8ba95f72cfc3c5b9e9f642f8d52a85697ba919dbc8d955386d497e9a157
    ->T(d) 241.0m/4.0h/0.17d, B: 0x68.. $3.5057(0.12179999999999991) 0xcd923f0e3d7e3dbfd80a8d87d4e3ed5ea86c3900f42d8b8a738b4a31a6e1859b
    ->T(d) 233.8m/3.9h/0.16d, B: 0xcd.. $3.5057(0.12179999999999991) 0x59be97e0cfcde2946a60c24e390e48de05d19c8fa75935b4232af28009e72865
    ->T(d) 245.2m/4.1h/0.17d, B: 0x8a.. $3.5057(0.12179999999999991) 0x0499caf5147bae631b7d13f8484bff2485eea75ca67395a5a7ca60e5c86750e2
    ->T(d) 231.6m/3.9h/0.16d, B: 0xcd.. $3.5057(0.12179999999999991) 0x8d0df91e6d3c29ebcf8d428b41ece7ba3c6d4aa065c9a96a00251f2a199f854e
    ->T(d) 240.3m/4.0h/0.17d, B: 0x68.. $3.5057(0.12179999999999991) 0x1af90f91b7334a17215660d67b6b8363bae3086b060d5281bc89a6181d7fe42f
    ->T(d) 236.7m/3.9h/0.16d, B: 0x23.. $3.5057(0.12179999999999991) 0x02fab012c727cc598955e9b47c630829e329d1997af2b7a72aa964d3d9953c79
    ->T(d) 236.4m/3.9h/0.16d, B: 0xa3.. $3.4498(0.06590000000000007) 0x66508437340fe84f77f98b9d9b079f2299883f846c9c1afa3336ee9529e4f4c0
    ->T(d) 234.8m/3.9h/0.16d, B: 0xa2.. $3.4498(0.06590000000000007) 0x9484966fe7e94d632a6baa5aff3cf39390b0a496d8921b664c33484e30b6d833
    ->T(d) 245.1m/4.1h/0.17d, B: 0x1c.. $3.4498(0.06590000000000007) 0x68eba8bc7d4d5b5a2b68b51a87b0c3c72e7ce13dad8ccfcdfdcfe403780ae77d
    ->T(d) 230.5m/3.8h/0.16d, B: 0x46.. $3.4498(0.06590000000000007) 0x960d9b1c4c75140efe77c25e2b6b341701e447bfe122a35927ec0ac48fab9be2
    ->T(d) 287.4m/4.8h/0.20d, B: 0x00.. $3.4498(0.06590000000000007) 0x1c03b8ea325d26ff6e726d57c03359e94a4eaa69f220da55af2b9668f50e89f3
    ->T(d) 195.4m/3.3h/0.14d, B: 0x6a.. $3.4498(0.06590000000000007) 0xa2b6337d2a28f02cb72b280e6056500fb107214738340c94c76e984baae627a0
    ->T(d) 234.1m/3.9h/0.16d, B: 0xf5.. $3.4458000000000006(0.06190000000000051) 0xca590b0792f15851e988d117bfe99f40a7137c6fae5f34b3915b95169b77949b
    ->T(d) 233.4m/3.9h/0.16d, B: 0xad.. $3.4458000000000006(0.06190000000000051) 0x140d1bdffc2ea154a427471dd77fda8b35350ccf636af69bd27712510b569767
    
    0x00c9da65b33b4f7034b5f50b90f5f6d8320d6ab8
       T(d) 0.0m/0.0h/0.00d, B: 0x00.. $26.5398(0.0) 0x329eafed68bde3b24149f7641f90c8be886c767f231dd4464cd02a27bb042d4a
    ->T(d) 949.9m/15.8h/0.66d, B: 0x83.. $26.5398(0.0) 0x6ccc96f9940646ee9e1cf1aa1c475d3e2c8b903e0192c7dd2ab796c4142e2a88
    ->T(d) 240.4m/4.0h/0.17d, B: 0x6a.. $28.1778(1.6380000000000017) 0x1a940af8b0ea4f7503ef5af3f920cce8b0293adea671736dc6b07f232551f542
    ->T(d) 235.7m/3.9h/0.16d, B: 0xf5.. $28.1778(1.6380000000000017) 0x1bfb123628dfd0c2c7fcf709f603cf95ce65bc9b06cf964fd638dca4ca6a102c
    ->T(d) 239.7m/4.0h/0.17d, B: 0x00.. $28.1778(1.6380000000000017) 0x95984fddcf6ddb4d35c67a96cda3a0d17d90d7f01e65dec0ee2dce3cfe8ddac1
    
    0xf5aee6d7b838d5ede8aa65d31dbc11116545180c
       T(d) 0.0m/0.0h/0.00d, B: 0xf5.. $38.831635(0.0) 0xbd464975655d426423333e2f21eb63ef47f16f158eaf06000168cc1f3a55bd75
    ->T(d) 236.3m/3.9h/0.16d, B: 0x6a.. $41.43778500000001(2.6061500000000137) 0xab8b2bb50e0321ebff8c59d0b15ab6dd397b72df200eb4784e2e34abd7d2703b
    ->T(d) 234.9m/3.9h/0.16d, B: 0xcd.. $44.04393500000001(5.212300000000013) 0x430a599bbdabcf1f65d783434aa8868b8df4a746723c8b8bf359ffb89f7e9e39
    ->T(d) 240.5m/4.0h/0.17d, B: 0xf5.. $44.04393500000001(5.212300000000013) 0xd94d0458a0285a9433dd36d31e349367b8f5d8470b582245ab8afb40eed83fec
    
    0xbabda06088c242fb2a763aa7cc99706cb77ba735
       T(d) 0.0m/0.0h/0.00d, B: 0xba.. $24.83856(0.0) 0x25a6ce1422156e30a90d02210d5da7e5a27a186a49db3396e8e12e5266585951
    ->T(d) 234.9m/3.9h/0.16d, B: 0x87.. $24.83856(0.0) 0xbaa269c2bc9fa0496f4ad7b34c9227843af4ddbffb859c47da2c95bd1949c18c
    ->T(d) 240.2m/4.0h/0.17d, B: 0xa2.. $24.83856(0.0) 0x0b41866ce977e8446cdc38f581b8014a61b180039086f7e96c3f8a4cdd0f610c
    ->T(d) 235.2m/3.9h/0.16d, B: 0x1c.. $24.83856(0.0) 0x0e0c0aead154d1b224429d54c9bc299ffd33cc7fd5b9f02058b8964bd57d2765
    ->T(d) 240.1m/4.0h/0.17d, B: 0x46.. $24.83856(0.0) 0xb0529cebf948244abec8f772529f9e4e8bd2803f0c1662fa119f8f58a7d6a511
    ->T(d) 234.9m/3.9h/0.16d, B: 0x00.. $24.83856(0.0) 0x7bdf38d83c83c69e93abb028e372fb209beef1aa38c4c9ec0b8f7eba407f85b2
    ->T(d) 240.9m/4.0h/0.17d, B: 0x6a.. $24.80976(-0.02880000000000038) 0x631137f0640e901bdd2ce986cca370bf9f66fd54d838e1479326f5606f8d1a44
    ->T(d) 236.2m/3.9h/0.16d, B: 0xf5.. $24.80976(-0.02880000000000038) 0xbf6125e3b7c2d6115ce1ac8036bbcb3eaf5daf442631a493f74ed2d26890ddea
    ->T(d) 238.2m/4.0h/0.17d, B: 0xad.. $24.80976(-0.02880000000000038) 0xb2c798f9c7af0ed524e16f14847a6f7c3f9e04b46d267337bc67c1cd01cbfe73
    ->T(d) 234.7m/3.9h/0.16d, B: 0x83.. $24.80976(-0.02880000000000038) 0x55e5fd54926be5b1ecfde950ff5021c35dc1722625ba932aa2a51a5b15d38d81
    ->T(d) 239.7m/4.0h/0.17d, B: 0x8a.. $24.80976(-0.02880000000000038) 0x05970071b7c0085a72f13fdad70b2fda71cae12a23e1ca762597eaeb7c10871c
    ->T(d) 235.3m/3.9h/0.16d, B: 0xe7.. $24.80976(-0.02880000000000038) 0xd9c5a17b0e60bb8359bc9485341fa0030c881bcf9296e5761a29af5600d27a9a
    ->T(d) 241.1m/4.0h/0.17d, B: 0xa3.. $25.21512(0.3765599999999978) 0xf76641f6f0e28482726329fbe28e9e7c2cec52b9d4532a213962e968440006ab
    ->T(d) 235.2m/3.9h/0.16d, B: 0x1c.. $25.21512(0.3765599999999978) 0xaf5593449444a18862632d9301327f08d2e4faea47d5a3cb6715503e9005e64b
    ->T(d) 713.4m/11.9h/0.50d, B: 0x8a.. $25.21512(0.3765599999999978) 0xc4dd695938c9aa4ad7093cc7ab0eb7caccdb84e12a42e8e886e45740c6aec749
    ->T(d) 235.8m/3.9h/0.16d, B: 0xa2.. $25.21512(0.3765599999999978) 0x93049b7736310ace93fc0ae8a6f9994eb7d161b07ff5bb0fd5502564026a47f3
    ->T(d) 240.8m/4.0h/0.17d, B: 0x73.. $28.073519999999995(3.234959999999994) 0x4a9e3fb97bda33d7425e8ab75ff929c5a4469cb24b1741a56dcb46b81ab356c4
    ->T(d) 234.2m/3.9h/0.16d, B: 0x1e.. $28.073519999999995(3.234959999999994) 0xcd66b3a07d865d45beb9934a921ce50ac1434b2699a38d8c67d9272f279577cf
    ->T(d) 238.9m/4.0h/0.17d, B: 0x6e.. $28.073519999999995(3.234959999999994) 0x1a3480d33464da2c6cff8ce967ff99c5c299d584e6e0d484810fd1c47533df64
    ->T(d) 235.3m/3.9h/0.16d, B: 0xf5.. $28.073519999999995(3.234959999999994) 0x4ed3aaa956b6c852072a2a2b37d5286dfa9dffb89e36c6c19d0d51f1f3deae74
    ->T(d) 239.9m/4.0h/0.17d, B: 0x00.. $28.073519999999995(3.234959999999994) 0xa48304349a4d2591b39bbf3d925f9830ec29eff6c05be65b772d9b4c274b7a02
    ->T(d) 236.0m/3.9h/0.16d, B: 0xa2.. $28.073519999999995(3.234959999999994) 0xb98d0343b1f3d52f24ea3170a01e10d423eaf84541fe7a6f5f73cb3f93264b0e
    ->T(d) 241.2m/4.0h/0.17d, B: 0x6a.. $26.83728(1.9987199999999987) 0xc5e1a3deb033cd10d64b53cf9b7c7b3aae8c78315dee8887cbc1707dfa8de536
    ->T(d) 233.1m/3.9h/0.16d, B: 0xa3.. $26.83728(1.9987199999999987) 0x43a03bad88d444faf4d11535e0793a12abba434511d0b1f3f490235f0dd3b6d2
    ->T(d) 239.9m/4.0h/0.17d, B: 0xf5.. $26.83728(1.9987199999999987) 0xeb30420c4b82c18d781f038ce06911b14e0f862e383689cd9bd0771f1c049f2e
    ->T(d) 234.9m/3.9h/0.16d, B: 0x6a.. $26.83728(1.9987199999999987) 0xd91af9b276d942e09a70cec9c04d77b5f1c6795d1d5c94f218402752d467f92b
    ->T(d) 240.2m/4.0h/0.17d, B: 0x87.. $26.83728(1.9987199999999987) 0xce35d3267939aee511f0ab66c881cec5d255bf5e71c7453a42104c04c9ae2a08
    ->T(d) 234.8m/3.9h/0.16d, B: 0xcd.. $26.83728(1.9987199999999987) 0x567629da74f39290471c8f611177a72d448db916fc63e301751bb75b39d4a23d
    ->T(d) 240.0m/4.0h/0.17d, B: 0x17.. $29.16504(4.32648) 0xf0a46092ab00c5667416d4d9126ef5858bc30e4bc4da4c05899f5460c817dad9
    ->T(d) 236.1m/3.9h/0.16d, B: 0x68.. $29.16504(4.32648) 0x5edbbd075132842709e25c7ead963150c02d9387b8da2286552829e3b022dccf
    ->T(d) 239.0m/4.0h/0.17d, B: 0x1e.. $29.16504(4.32648) 0x3add2d0cbd247d2b2029f754e038bd4c3e032acd1bf5abcce11ac02bd99a6a73
    ->T(d) 1428.0m/23.8h/0.99d, B: 0xa3.. $30.222719999999995(5.384159999999994) 0xa688637d26a181520d13958a9f59f8d2c37fc64f6f5b76c1e97023122eb21eb2
    ->T(d) 233.6m/3.9h/0.16d, B: 0x1c.. $30.222719999999995(5.384159999999994) 0xc4cc8fd99f6b863482d47db3a800bf3d6f4aa8a5686113c8cb93ccaf99024b96
    ->T(d) 238.5m/4.0h/0.17d, B: 0x23.. $30.222719999999995(5.384159999999994) 0xe31ee7bdb743022e39e2c63ec5370a7bc24b4d18546013b5c53abd9e05ca439d
    ->T(d) 235.2m/3.9h/0.16d, B: 0xba.. $30.222719999999995(5.384159999999994) 0xfdc141b6074dc03c87d9d4cc8c8db9f14156a46ebe42376558906ddfb263b018
    
    0x7316e9cf94bef40d4981d66a5c41c38b6b32454c
       T(d) 0.0m/0.0h/0.00d, B: 0x73.. $32.217119999999994(0.0) 0x1e0e021b454445933b950097995f0e60f46636e6e247b582f70b133895de2f29
    ->T(d) 235.5m/3.9h/0.16d, B: 0x87.. $32.217119999999994(0.0) 0x69a1b9a0d511b827c2d3ed346357b11d67b610abc098c901956cf5fee57bd470
    ->T(d) 239.6m/4.0h/0.17d, B: 0xa2.. $31.8825(-0.3346199999999939) 0x51e855f129b75e87c954ee38c4ecc57db70bfb91d9526b0adc0e862d24672779
    ->T(d) 234.6m/3.9h/0.16d, B: 0x46.. $31.8825(-0.3346199999999939) 0x1b6f76e401ef0864094c9e3d0c5eb67d78e29a55209fb402ac84a33da7826713
    ->T(d) 240.0m/4.0h/0.17d, B: 0xa3.. $31.8825(-0.3346199999999939) 0xe3c7dc0f497f668a2fa653b8ac9ea7fba77285ae58f4a21f9db50e185da0ea71
    ->T(d) 234.9m/3.9h/0.16d, B: 0x83.. $31.8825(-0.3346199999999939) 0x33a22a350a611f034443e57f357c086b873e35b1501db624524bac0b3e87b7cb
    ->T(d) 241.6m/4.0h/0.17d, B: 0x8a.. $31.8825(-0.3346199999999939) 0x26b9aa04c5603314579709feda03d8fc66212968d343752feef17df8467d966c
    ->T(d) 234.4m/3.9h/0.16d, B: 0x83.. $31.8825(-0.3346199999999939) 0x5e76e2635155834b8a0b4245b3188e46a190869f45be8aa0d91d9ca0d2474df5
    ->T(d) 239.5m/4.0h/0.17d, B: 0xe7.. $31.15592999999999(-1.0611900000000034) 0x4b14fde4e586aa97fad45a3fc507561ac8d7c255d1a1ae607905d8af0b85471b
    ->T(d) 234.5m/3.9h/0.16d, B: 0x23.. $31.15592999999999(-1.0611900000000034) 0x4a8d09e26ad8a1f3ee860bff17c2160ee56a0f16605a0171b05ac8b5aeef5370
    ->T(d) 240.2m/4.0h/0.17d, B: 0xa3.. $31.15592999999999(-1.0611900000000034) 0x68893c4e1ced67e0fc6855bbd9b8e7512e99e137d3c1b802da04939824ff0630
    ->T(d) 235.8m/3.9h/0.16d, B: 0x17.. $31.15592999999999(-1.0611900000000034) 0xb7add650fbc7da980db7cde2beebe2659ffec0aca4116ba5b3ed2327f464f479
    ->T(d) 240.8m/4.0h/0.17d, B: 0xba.. $31.15592999999999(-1.0611900000000034) 0x4bc476a6a6f2b7d750caa360af707fd5c665c9067e38ef75cd6b7d208ef838f4
    ->T(d) 235.3m/3.9h/0.16d, B: 0x68.. $31.15592999999999(-1.0611900000000034) 0x99a617df5ee4c40378b468753d9a5203dcc0b707675aa1be32af8e423f11f90e
    ->T(d) 239.4m/4.0h/0.17d, B: 0xa2.. $31.13721(-1.0799099999999946) 0x7677c8ad3ddd0fd3208601d2d6eac01d2dd85641e113588b300c81af60b7443d
    ->T(d) 950.0m/15.8h/0.66d, B: 0xcd.. $31.13721(-1.0799099999999946) 0x37fdbb0ab1deb7561b2be501f0162cd912832c739f4f111163a0f3b3ecb820b9
    ->T(d) 235.8m/3.9h/0.16d, B: 0x83.. $31.13721(-1.0799099999999946) 0xdea322bd53efe423970c64bbe7238c6e14335fc7668e6fd722a1a89ec19b3a56
    ->T(d) 239.7m/4.0h/0.17d, B: 0x1e.. $31.03893(-1.1781899999999936) 0x496e52e694377fc0e9a43a62b11090ddeeec7754c714e2b95c85f2baa2251f98
    ->T(d) 234.0m/3.9h/0.16d, B: 0xcd.. $31.03893(-1.1781899999999936) 0xe084fe4397a8aa889df50f85a6f400f0eb1b73327ace3233a7dc7f5a03b3a482
    ->T(d) 242.0m/4.0h/0.17d, B: 0x8a.. $31.03893(-1.1781899999999936) 0xce1994c4ba43b4776e880902c86753ec1bbd76c6beccc40ffa9bd8de76ca0891
    ->T(d) 242.6m/4.0h/0.17d, B: 0x1e.. $31.03893(-1.1781899999999936) 0xc193bdc8d90ef06f945c4909ec65c3c4252987445042209b06b76dee93366abb
    ->T(d) 229.8m/3.8h/0.16d, B: 0x8a.. $31.03893(-1.1781899999999936) 0x47abee6f59fd11f6667510ff841d39d4eb0c0b7382087d67d3fcf956b1995015
    ->T(d) 236.4m/3.9h/0.16d, B: 0x46.. $31.03893(-1.1781899999999936) 0x5327228dff500408078167faecb97a5e77e4fb98c12984cd8bd89bef66288b5d
    ->T(d) 238.8m/4.0h/0.17d, B: 0x8a.. $31.02605999999999(-1.1910600000000038) 0xb6104f99c437fa2f9975fe9af667b010f4af25016ae02592942d2adcf0100599
    ->T(d) 234.9m/3.9h/0.16d, B: 0xba.. $31.02605999999999(-1.1910600000000038) 0xc6cc4131fbfb57733bb4aa954b319a4b3c3cedbf1ce19a2cee75df892f358047
    ->T(d) 240.5m/4.0h/0.17d, B: 0xa3.. $31.02605999999999(-1.1910600000000038) 0xa7e8df65f342974bf17a4985e10d64e8b6866ae1dca367b1c1cca5cede932b2d
    ->T(d) 235.2m/3.9h/0.16d, B: 0x1c.. $31.02605999999999(-1.1910600000000038) 0x436bcaaf132422e2b6d91b468ebba9bb9a03084858ffb8bf40259bed64b8fe93
    ->T(d) 240.1m/4.0h/0.17d, B: 0xe7.. $31.02605999999999(-1.1910600000000038) 0xecd9305dc42aee37906f26787faf0639525cb99675167dcbbaff11f3d199100b
    ->T(d) 235.4m/3.9h/0.16d, B: 0x1c.. $31.02605999999999(-1.1910600000000038) 0x66f8b440e68a28faeaf3ab8789e87ae0ffcc5c9367911adad458f0a4f4049b76
    ->T(d) 1188.5m/19.8h/0.83d, B: 0xe7.. $31.73975999999999(-0.47736000000000445) 0x3132ea2c02057ff83263368f6c27395de8f07f130c96317f169bbedae90bc638
    ->T(d) 235.2m/3.9h/0.16d, B: 0x1c.. $31.73975999999999(-0.47736000000000445) 0xfcc5199bea22d1b249a994c0359c90dcbce7547755007195d7c5b1ad0f7f9421
    ->T(d) 716.1m/11.9h/0.50d, B: 0xe7.. $33.197579999999995(0.9804600000000008) 0x36035c0a38db0ac1d5730a295ea7c701e28aa4c07fe7c0d167d3d88f54c13fdc
    ->T(d) 238.8m/4.0h/0.17d, B: 0x1c.. $33.197579999999995(0.9804600000000008) 0x5498ffb87491f509cde816a64b3193373b8593ca00c8bd48021da9116ce2348a
    ->T(d) 235.4m/3.9h/0.16d, B: 0xf5.. $33.197579999999995(0.9804600000000008) 0x227b717c245d02b0205191044188a6b4dcfd47757cda4f17cd613fee54596068
    ->T(d) 235.3m/3.9h/0.16d, B: 0x83.. $33.197579999999995(0.9804600000000008) 0x1f50389f7af90869dd9045c855de37ad691dea1df52c635a53f92611f4ea8dbf
    ->T(d) 240.1m/4.0h/0.17d, B: 0x1e.. $32.66874(0.45162000000000546) 0xdf7ebfaf928049ce19a94cd3874a122bf86eed19f7e2f93cbef3f8f91764f00b
    ->T(d) 234.7m/3.9h/0.16d, B: 0xa3.. $32.66874(0.45162000000000546) 0xe3be034ca5a34f2ab092a4b9b47abfcb3815ea5c86c4463c833f5831621e8bb4
    ->T(d) 240.0m/4.0h/0.17d, B: 0xad.. $32.66874(0.45162000000000546) 0x8a5d70c99614313c29b943ddfbbc639fe15d4f6f7c8db6cb2993bccf1737be1d
    ->T(d) 236.5m/3.9h/0.16d, B: 0x46.. $32.66874(0.45162000000000546) 0x43408867b15189d86cd0f1b37ab1ede52216f161ae7a210a57e8360b3b1ec29e
    ->T(d) 237.9m/4.0h/0.17d, B: 0x23.. $32.66874(0.45162000000000546) 0x6674d73832dd06c8c3a4c964ccc00b571a1756f0fa775ef649fb3df5c723c616
    ->T(d) 235.3m/3.9h/0.16d, B: 0xba.. $32.66874(0.45162000000000546) 0xb47e3c0d607a5e86c85e8283b10e6e5fc56d55eb2187d503870dcf18d17f8f92
    ->T(d) 241.8m/4.0h/0.17d, B: 0x87.. $32.49674999999999(0.2796299999999974) 0x3770d31fc05105fcc85a7dff8580ec8bfb80df29f52fcfb95cd7757132b5622e
    ->T(d) 234.7m/3.9h/0.16d, B: 0xe7.. $32.49674999999999(0.2796299999999974) 0xb3e4b1d18e4fb0be00956e711e6cadfd34972ccb87368e294bc2800cb927aa7f
    ->T(d) 239.1m/4.0h/0.17d, B: 0x00.. $32.49674999999999(0.2796299999999974) 0x06f1cf857b2531db0e87914378b220a64725a7528b5dd8100fd230b01c060f04
    ->T(d) 234.8m/3.9h/0.16d, B: 0xa2.. $32.49674999999999(0.2796299999999974) 0x579e999f759e552610354b0a17c0d3c1f12fb6fb1adaf3839030aaefc8aa051e
    ->T(d) 1665.6m/27.8h/1.16d, B: 0xa3.. $32.58566999999999(0.36854999999999905) 0x7022c1c2f6db9e068e3fe8a100f01a4546a035679504227d4e4e06c2eff8b45f
    ->T(d) 236.7m/3.9h/0.16d, B: 0x73.. $32.58566999999999(0.36854999999999905) 0xa083b7695cb78cbdfd0ca2715200e1839912086a035f240b6eb4285404351d6e
    
    0x179d698f5a1c84c3ff4c5eb04e553c15a0c1d8d8
       T(d) 0.0m/0.0h/0.00d, B: 0x17.. $25.81524000000001(0.0) 0x214f16d3025ddd286d22b2b1ddd426286ab025d32f2dce4046ff3fa2dcb7120a
    ->T(d) 239.6m/4.0h/0.17d, B: 0x1e.. $25.81524000000001(0.0) 0x7c6d67f934f3e7cebe272516154dfd8f9c0bf7ded61b949ab3d96b8eac30bce1
    ->T(d) 234.6m/3.9h/0.16d, B: 0x00.. $25.81524000000001(0.0) 0xe1549015777681679e503451dacb558e784dac50004185a78b6edd2838e587cc
    ->T(d) 239.8m/4.0h/0.17d, B: 0x1c.. $25.81524000000001(0.0) 0x0048e068293f1d0bf14bbd02d94bdd664ba9f724bb934420461b281d6a2b0634
    ->T(d) 235.2m/3.9h/0.16d, B: 0xa2.. $25.827960000000004(0.012719999999994513) 0x45f4d23bf5a98b8a8d1a72abe5d421e243f5e7a03704145882e0b086ca22b026
    ->T(d) 241.1m/4.0h/0.17d, B: 0xcd.. $25.827960000000004(0.012719999999994513) 0xcee71258464971f37bf8cb3c1b4bb18bd2d9f86e5948d2283c398968312bf10d
    ->T(d) 237.6m/4.0h/0.16d, B: 0x1e.. $25.827960000000004(0.012719999999994513) 0xcacdd3e95e55deb522e379ca49ce7c7d9c35ece08fe2e4ee8d0314781194e23e
    ->T(d) 236.2m/3.9h/0.16d, B: 0x23.. $25.827960000000004(0.012719999999994513) 0xd9ad64d012a0898d7f407b0676374f8da7048b7b1344e5419004603a1a114b4d
    ->T(d) 235.6m/3.9h/0.16d, B: 0x73.. $25.827960000000004(0.012719999999994513) 0xe5d358e9a962bcf236c2c1582b64d14a8042205b804b18fcdb5bb86047c9eb31
    ->T(d) 240.7m/4.0h/0.17d, B: 0x6a.. $25.827960000000004(0.012719999999994513) 0x7d57cd07799ca3067e1e97f0073cd3d74752728d337d87e3df2c9bcd3acbbb34
    ->T(d) 233.6m/3.9h/0.16d, B: 0x17.. $26.418645000000005(0.6034049999999951) 0x43486da9ec4376917772094c0d0480b87fba76ff4ced04fa8b7a5f4f8f87a846
    
    0xa37e6b46fa8e1a6f1ddbf035c4e0230b8414ff04
       T(d) 0.0m/0.0h/0.00d, B: 0xa3.. $24.12060000000001(0.0) 0x40bfcd18ef94a11357ab488d4c712ed5709d45b3dabe2585ed85b2f92d92c26b
    ->T(d) 240.4m/4.0h/0.17d, B: 0xe7.. $24.12060000000001(0.0) 0x70ca093faffcb79d4e397e8a6defa781af5e7f6e7418399126232b5d4938cdee
    ->T(d) 239.2m/4.0h/0.17d, B: 0xba.. $24.514700000000005(0.39409999999999457) 0x69fbb33a3c2ca6c3247f46a1f3f1907a8e66651e3d0a34dee6523ca61ea15ea0
    ->T(d) 236.0m/3.9h/0.16d, B: 0xf5.. $24.514700000000005(0.39409999999999457) 0x4404b488d9487bdef34875de3f08f77bda3a1f9ff951af441deac76b27b5b926
    ->T(d) 240.7m/4.0h/0.17d, B: 0x87.. $24.514700000000005(0.39409999999999457) 0x97ae94642aa5821d46c2b6bf7eb88f2a51208ecadd416bebfc1354d6e55a7292
    ->T(d) 708.9m/11.8h/0.49d, B: 0x6e.. $24.514700000000005(0.39409999999999457) 0xc19071eec9aa8a49231f14c36941a270c3b2959017f201fed55777fc372baf6d
    ->T(d) 235.2m/3.9h/0.16d, B: 0x8a.. $24.514700000000005(0.39409999999999457) 0x269183d0a8b59088df36cb1d5a52b8f02677342022f78f813a1bdf929589d16b
    ->T(d) 240.2m/4.0h/0.17d, B: 0x68.. $27.2937(3.173099999999991) 0x55e9f902fa86bb38c9cb1927d65d1dd0516b6fc8734f16ab1f22ad6bc11e9b00
    ->T(d) 234.8m/3.9h/0.16d, B: 0x46.. $27.2937(3.173099999999991) 0x9ac1a6e6c4bf717a4379eb035e91842d8fcf360686a2a74697cbce98ab74f6f3
    ->T(d) 239.8m/4.0h/0.17d, B: 0x23.. $27.2937(3.173099999999991) 0xa0f26b8f553206c6366167f08798317d5f8b606f22193ba04bdfe0acee6c29b6
    ->T(d) 235.5m/3.9h/0.16d, B: 0x1c.. $27.2937(3.173099999999991) 0x9bd16bfd0af1ff952f6d053b0704d101a922f4bc906f1e7b3086f0edd1f39f18
    ->T(d) 239.4m/4.0h/0.17d, B: 0x87.. $27.2937(3.173099999999991) 0x47b9d41b39f65add96da2cbaf9a2ade2bcb66491e5f9e6c7a52cf4f0d5840795
    ->T(d) 235.7m/3.9h/0.16d, B: 0xad.. $27.2937(3.173099999999991) 0x736c3bbb1a5b63e212bee74511b663c2ab96068cfd327191d12ae688e132cb77
    ->T(d) 239.8m/4.0h/0.17d, B: 0xe7.. $26.091800000000006(1.971199999999996) 0x4866f09df9b722be4198f9ae09cf35a793c53552c0d2cc9a6e79ebd15f522561
    ->T(d) 235.8m/3.9h/0.16d, B: 0xcd.. $26.091800000000006(1.971199999999996) 0x6f60ccdcd261737c34682b3aec79f27d5f35f2842b9cee744a1af52a4dd7943b
    ->T(d) 238.8m/4.0h/0.17d, B: 0x17.. $26.091800000000006(1.971199999999996) 0x27f8843f1097fd8ae0b453aafa38d31e2e561576c69729a53fa48141c254ff6c
    ->T(d) 234.8m/3.9h/0.16d, B: 0x68.. $26.091800000000006(1.971199999999996) 0x9da1152f5fecc0e372fc694de0eb658376a46eb2d8cae69d5789a9782c9f09db
    ->T(d) 241.9m/4.0h/0.17d, B: 0x1e.. $26.091800000000006(1.971199999999996) 0x8b452ec72869204f2b0e4e55a3a44d8d4e4037325a874ad1fe743df072492b46
    ->T(d) 233.1m/3.9h/0.16d, B: 0xe7.. $26.091800000000006(1.971199999999996) 0x10999b8e62b0773c15753a5ffa59c8050f8fcb682b5d281f74412d885eb70239
    ->T(d) 240.1m/4.0h/0.17d, B: 0x73.. $28.3549(4.23429999999999) 0x0afbf3def459234ef0b239feeae57f38a56bafae87f0f6285355868cd6d1704c
    ->T(d) 239.2m/4.0h/0.17d, B: 0xad.. $28.3549(4.23429999999999) 0x20e5c8351c2276deb9e5329cb1605fc314f009185879a488cf3ddd806bb6136f
    ->T(d) 235.8m/3.9h/0.16d, B: 0xba.. $28.3549(4.23429999999999) 0xf75b34802d9afeea8dca24b62b378ac1b3c4476aea10ebaab6dcf7c8cc497f53
    ->T(d) 1424.7m/23.7h/0.99d, B: 0x68.. $29.3832(5.262599999999988) 0xae1ecfd25031ebe108211a1da0bd824331fa20ff05a3ae6eac319bf9a8f07a06
    ->T(d) 238.9m/4.0h/0.17d, B: 0x46.. $29.3832(5.262599999999988) 0x3190eb3e79b7f923a3cf9b22657d92565f021b4ded804c74df290279dd5e748e
    ->T(d) 236.9m/3.9h/0.16d, B: 0x73.. $29.3832(5.262599999999988) 0xb7c22362d4c5aaaa53698fd716396a2c5c9f39363f13c683271f79c3e0747355
    ->T(d) 235.0m/3.9h/0.16d, B: 0x1c.. $29.3832(5.262599999999988) 0x6eac3da3287125a13271f44f7f53fa54f26da7b2b31a761fd290b88289d69633
    ->T(d) 239.5m/4.0h/0.17d, B: 0x6e.. $33.0771(8.956499999999991) 0x515cdf79f18169de9e7e2fed05747b11a06364655cf0ef46a4ce4958f583184c
    ->T(d) 235.1m/3.9h/0.16d, B: 0x83.. $33.0771(8.956499999999991) 0xabdff7737653024cbf7a0db0a9aafe0ac4f05f64a01ec8e5258fdaff58ce658d
    ->T(d) 315.9m/5.3h/0.22d, B: 0x1c.. $33.0771(8.956499999999991) 0x0de63adbe9987100fe11e8decadeb8b6331a3fa79d053876973a48a8b2a8c911
    ->T(d) 159.9m/2.7h/0.11d, B: 0x00.. $33.0771(8.956499999999991) 0xd8988070631f47929f405c72d2a192e614b0abd8606e26f4b238bfa8421a1778
    ->T(d) 239.0m/4.0h/0.17d, B: 0x6a.. $33.0771(8.956499999999991) 0x8e014f2ed9f60549fbbfe81da989769a2425fbc97375d016d60c789c72402305
    ->T(d) 235.5m/3.9h/0.16d, B: 0x87.. $33.0771(8.956499999999991) 0x93f2f6431c12f2a5e69bb37a38f0abdf30c6ccc3101c0c316522f9ec630e6a7b
    ->T(d) 294.4m/4.9h/0.20d, B: 0xcd.. $35.83230000000001(11.7117) 0x8cd7a0cbb992ff3899c7c8611583499b5f47de2db1e320dc1ec0e623a536dd06
    ->T(d) 424.3m/7.1h/0.29d, B: 0x17.. $35.83230000000001(11.7117) 0x9dc755377709f63bce0283977b3abd55ad553de185925f07124d3254c7716613
    ->T(d) 231.6m/3.9h/0.16d, B: 0x8a.. $35.83230000000001(11.7117) 0x6f420d28c04ce47461eb8fdc56b632dda24d419fa9e5310daeb224910c56b57b
    ->T(d) 239.4m/4.0h/0.17d, B: 0x23.. $35.83230000000001(11.7117) 0x8538890c3e04a29c97db62f5772128052c7fe1858457e53508f96d4330d83c3b
    ->T(d) 234.9m/3.9h/0.16d, B: 0xe7.. $35.83230000000001(11.7117) 0xcb3dd89a89e672bce612d8e887738bc513c61d098f77b9befe20fe7be9c9848f
    ->T(d) 241.1m/4.0h/0.17d, B: 0x8a.. $33.7953(9.674699999999987) 0x68c6c791bef3d29590a95a1116f32705a1813d722bffac936818f663d7697a98
    ->T(d) 233.9m/3.9h/0.16d, B: 0x23.. $33.7953(9.674699999999987) 0x35224c49c7ef76cc60b26384f094367a8c4788cf723093653c5c1c55cc85d2cc
    ->T(d) 240.0m/4.0h/0.17d, B: 0xe7.. $33.7953(9.674699999999987) 0xe595b6d045946b815b01ffe476bf3b701f3538b24df7347568aa778259a9f946
    ->T(d) 234.8m/3.9h/0.16d, B: 0x17.. $33.7953(9.674699999999987) 0x3e6a443709e6907400cfcf3dbddd44754b2a155a867870bb843f310fb37484b9
    ->T(d) 240.3m/4.0h/0.17d, B: 0x1e.. $33.7953(9.674699999999987) 0xea388c8bf5d08c36177a36e393207c8b81daae928bc2285f2c4cbc78c4277f24
    ->T(d) 235.1m/3.9h/0.16d, B: 0xa3.. $33.7953(9.674699999999987) 0x6fbab7f2d4390ef25b03bdaf6bb031940754a21e21275dfc366981774bf7d3da
    
    0x236ef21dc36d0aec3990dd5ee84a9d5320644262
       T(d) 0.0m/0.0h/0.00d, B: 0x23.. $71.17105000000002(0.0) 0x0a8c9672be0475dfa915b121688c6e11d23af243cd784632f555fcd4ce825c54
    ->T(d) 235.4m/3.9h/0.16d, B: 0xcd.. $71.17105000000002(0.0) 0x91a0b2ad2fe680c9f5cce76dc8bb9a532a85189d402d0d464bf9a6899f734348
    ->T(d) 239.8m/4.0h/0.17d, B: 0x1e.. $71.17105000000002(0.0) 0x414cd57a004699e683603a363075272be155b7293185bec866824fa5e9e862fb
    ->T(d) 234.7m/3.9h/0.16d, B: 0xa2.. $73.045925(1.8748749999999745) 0x23beab369912cef65bf311cb9989c1fa573016ac779ba49b0af825781a62e5f1
    ->T(d) 240.5m/4.0h/0.17d, B: 0xa3.. $73.045925(1.8748749999999745) 0x2595f9a76565f840df309af76d980f681aa5e96529255e68300afaefd6e8e23f
    ->T(d) 234.9m/3.9h/0.16d, B: 0x00.. $73.045925(1.8748749999999745) 0xf20aae9af2fa4905ccca603f2cdbb54d01e5b43e1a3cc4866d4b8c052e424d23
    ->T(d) 239.9m/4.0h/0.17d, B: 0x1c.. $73.045925(1.8748749999999745) 0xac11eaf21eaf01b42b66cb4d2ceeb8da531d544df9ba8230fb9640f726cbef86
    ->T(d) 234.7m/3.9h/0.16d, B: 0xa3.. $73.045925(1.8748749999999745) 0x8050d087c38c1606c8971d2e904d8482f7c506b71793993afdc2c0def41bc3bf
    ->T(d) 241.4m/4.0h/0.17d, B: 0x23.. $73.045925(1.8748749999999745) 0x31d86d3f252c1a24bd3cf629978f8ab5b19ca0236a21b0a465dfcda34d817e95
    
    0x838c14eb3eabe4cb6a696d286c7b2a466629d0ee
       T(d) 0.0m/0.0h/0.00d, B: 0x83.. $24.012200000000004(0.0) 0x0e7861cd7bb846394979c8cb4821dce50715ae72e15625e0c73363b6a2734e74
    ->T(d) 234.6m/3.9h/0.16d, B: 0x6a.. $24.012200000000004(0.0) 0xc7d9bff2e8a1425d4287f6042499070aeabd0f8595b357b3c1b76bb67972c3fa
    ->T(d) 240.6m/4.0h/0.17d, B: 0xf5.. $25.49420000000001(1.4820000000000064) 0x3fc359e609ee2ea46ed67a9586c0bd7bccb0feb280c03027ca3e6f69546329bf
    ->T(d) 234.6m/3.9h/0.16d, B: 0x00.. $25.49420000000001(1.4820000000000064) 0xb6741d2e5611d337abcaff5f39ebc2a2956e670da20c55ec10819c0e96c4a1c1
    ->T(d) 245.7m/4.1h/0.17d, B: 0xa2.. $25.49420000000001(1.4820000000000064) 0x583defb926309a41591ddff8a0af675bd9a0786900c0a07d465f2254986630ab
    ->T(d) 229.5m/3.8h/0.16d, B: 0x00.. $25.49420000000001(1.4820000000000064) 0xc3feba1ddb70690612695dd5bbf460ba8a74b3e1fdf9d5f69ef5f90529af6bda
    ->T(d) 239.7m/4.0h/0.17d, B: 0x73.. $25.49420000000001(1.4820000000000064) 0x1ef2a1a63ce36663a133bf4ca4f3e444b913a8b7bf036be9ce28562936c29da8
    ->T(d) 235.8m/3.9h/0.16d, B: 0x46.. $25.49420000000001(1.4820000000000064) 0x9bfbd854ceca8fcee90285608549d39197a0e8049a614787b96041d1d386aee4
    ->T(d) 239.7m/4.0h/0.17d, B: 0x23.. $26.144(2.131799999999995) 0x973e38ea0f4725877344bc2be47545c55e7323ec5d2379815191268b2b01ff7d
    ->T(d) 234.5m/3.9h/0.16d, B: 0xba.. $26.144(2.131799999999995) 0xf493ed5563685382cc0cec1fdb7b44385c1f0664e2939e3dcde430c925e0c292
    ->T(d) 240.0m/4.0h/0.17d, B: 0x1c.. $26.144(2.131799999999995) 0x97c35c9cf37870669947970bd91fbd3307f4b83e7689dba85a9f2f1a08fe97b7
    ->T(d) 235.1m/3.9h/0.16d, B: 0x83.. $26.144(2.131799999999995) 0xe327d11a23bb52a2d68338299a39d242b8e8cfc148c4d3d0a626bbd7fc9ceacc
    
    0xcdcadf0279ee021a0c40a31ac10fa69e028e21d0
       T(d) 0.0m/0.0h/0.00d, B: 0xcd.. $2.9146(0.0) 0x0a9b72e8019e84ff6ff45b4bc9fb63b1803dd5bf4523cbf15737c564bfa97364
    ->T(d) 231.1m/3.9h/0.16d, B: 0xba.. $2.9486000000000003(0.03400000000000025) 0x3d95180694e8ed8d82fbdc959982ff2536911ab3ad094d4daf56bcdd03eb6e8e
    ->T(d) 239.3m/4.0h/0.17d, B: 0x46.. $2.9486000000000003(0.03400000000000025) 0xfd48da2e0b44b33aeaab759641a366fbd16c844238fcd174870fa5347dcdc52e
    ->T(d) 235.7m/3.9h/0.16d, B: 0x8a.. $2.9486000000000003(0.03400000000000025) 0x790b63eb14a6dab7caf8f75af9487f1f22849b04307f49dd01715a811e2140ff
    ->T(d) 239.4m/4.0h/0.17d, B: 0xba.. $2.9486000000000003(0.03400000000000025) 0x7daad146d6c7e2c5f101b6d3339c45b59db1f71a58d5a2fc32683a492f8d72bd
    ->T(d) 234.5m/3.9h/0.16d, B: 0xcd.. $2.9486000000000003(0.03400000000000025) 0xeff5f0ec3aa270c5f0e0d1faed216ff71e41dd7703e39056a7a40860edd03142
    
    0x6a47b60c377450a460d3eb828d534ee66eead668
       T(d) 0.0m/0.0h/0.00d, B: 0x6a.. $8.79814(0.0) 0x6d9d8b3a76538a95eab1ec090a22e28b1a90f3682d554bdc2a4fcf303e0cf806
    ->T(d) 240.5m/4.0h/0.17d, B: 0x6e.. $9.11482(0.31667999999999985) 0xe93717a93fe3f91ce228320e0a484947f6685eef47bfddc165286bfba50e1037
    ->T(d) 235.9m/3.9h/0.16d, B: 0xa2.. $9.11482(0.31667999999999985) 0x3084973929a0e2702c34fdc37ff58f1be194b095b7aa40c1b3da5cab03dfcdbb
    ->T(d) 239.3m/4.0h/0.17d, B: 0xf5.. $9.11482(0.31667999999999985) 0x15754d571f358cfa0a24b7fe4e69b6f716f13a753a19c09a3d298ba475be6302
    ->T(d) 234.2m/3.9h/0.16d, B: 0x6e.. $9.11482(0.31667999999999985) 0x82911673d8bd6c51ae4e7d91fa095c130d567fda668ff8aac089cb5d0400c0b8
    ->T(d) 240.2m/4.0h/0.17d, B: 0x46.. $9.11482(0.31667999999999985) 0x4bf3b35c22b74032c7463e7ccfe687ce4732ff56362cca4cf8a140110c428b59
    ->T(d) 238.2m/4.0h/0.17d, B: 0x17.. $9.11482(0.31667999999999985) 0xa3a6c60c25f7b39ddd263ebf94361659d50c5b002daf9acfebcc4880a3c6b707
    ->T(d) 237.0m/4.0h/0.16d, B: 0x68.. $8.96948(0.17134000000000071) 0x9fdb28ae900aec4885ec0333c69fef19a6e7cd8509aff7f27646453b1d4e73de
    ->T(d) 234.9m/3.9h/0.16d, B: 0xa3.. $8.96948(0.17134000000000071) 0x6eb82f791ff1918e11b34d81bdedff154f1d1260b2ca825455cd2044239ef960
    ->T(d) 239.9m/4.0h/0.17d, B: 0x1e.. $8.96948(0.17134000000000071) 0xdb8b6719601900a38d818f751e28934b8fc91023f741e8fff87cf01fff37b2ad
    ->T(d) 237.3m/4.0h/0.16d, B: 0xba.. $8.96948(0.17134000000000071) 0xb4607dc7e0d0660889297bf5896f8e3b7d3bbce5a518c1679abf2b8f21fe3679
    ->T(d) 238.1m/4.0h/0.17d, B: 0x73.. $8.96948(0.17134000000000071) 0x0b8edd6a030e5cc0d9046614f1a8dd8e16c37882bdccfc37d0222b50d532d05c
    ->T(d) 239.1m/4.0h/0.17d, B: 0xa2.. $8.96948(0.17134000000000071) 0xa8b530c6e9861019bf7f374b428689fcf87b8c1186232af19836109cd61eb1ba
    ->T(d) 237.6m/4.0h/0.17d, B: 0x23.. $8.95908(0.16094000000000008) 0xafa710d27d46b763f1975cae460dc3f7ad9902e3587fcd599172e31d1d40cf7c
    ->T(d) 233.2m/3.9h/0.16d, B: 0x46.. $8.95908(0.16094000000000008) 0xc55d14dfdfbce48c790d91b94ada61ad0451c3eb6d9fa75cfb29ac2702f0dfb8
    ->T(d) 240.4m/4.0h/0.17d, B: 0x00.. $8.95908(0.16094000000000008) 0xb1a52a135f19bc1765dc299b5e2de3e8bb7fedd652c04a30f59052194250c433
    ->T(d) 237.9m/4.0h/0.17d, B: 0x87.. $8.95908(0.16094000000000008) 0x07aedceb2ab0397539227c023af059c0004f3f0806c04b69a35e423e196620f9
    ->T(d) 236.6m/3.9h/0.16d, B: 0x6a.. $8.95908(0.16094000000000008) 0xa67456cb3b48783f1b3171be3039b9f1d2079c40681d1f9aafc2a2e6acf0b582
    
    0x1e7f320cf5a938465d501f6bd6c405feb3a70f6c
       T(d) 0.0m/0.0h/0.00d, B: 0x1e.. $21.358220000000003(0.0) 0x3cc952bfdc83ab01a64d132eb1c63d46bedd9a51fe3e3eac7d5949d7dbad7c8d
    ->T(d) 235.7m/3.9h/0.16d, B: 0x68.. $21.358220000000003(0.0) 0xbba004114cdaa0a24064cc0a18bfcfc4bf004fab647c082261b3ebbdab6ffe7a
    ->T(d) 239.4m/4.0h/0.17d, B: 0x1e.. $22.676420000000004(1.318200000000001) 0x169e6b1c5eff838c702fdc0fb14314d6887eaec3ad0ef2153bf1fafa4654cfc0
    
    0xe738725cdcc41c91f734dd7b5b9659df994d6dda
       T(d) 0.0m/0.0h/0.00d, B: 0xe7.. $11.63424(0.0) 0xd765b0e86322d82886101d1af41e37255c231eefdef00d779bebd568ab58b431
    ->T(d) 238.8m/4.0h/0.17d, B: 0x17.. $11.63424(0.0) 0x3b9fc33f68ec1a82edeabad3631b301060182bff956ab69aa8294789d04d4a72
    ->T(d) 709.9m/11.8h/0.49d, B: 0xe7.. $11.73216(0.09792000000000023) 0xc1374161258f700cf09109be14b1b8516200163375693cde728af9fb535feaea
    
    0xa21e0974137bf8400eb7dca606d9dcb190d79ed9
       T(d) 0.0m/0.0h/0.00d, B: 0xa2.. $28.2204(0.0) 0x1d93479b51bd107e888659beaa8d92bec9375c83c8d59c564ec6be5febe5d9c3
    ->T(d) 234.8m/3.9h/0.16d, B: 0x46.. $28.2204(0.0) 0x925b70ef8b6969bfd88948fff11b619dfa81607c4afabd16978cf33f9ba0fae0
    ->T(d) 240.5m/4.0h/0.17d, B: 0x8a.. $28.2204(0.0) 0x12343a8de5e9d0cbcd665c621744f1c8abb7dc0a299a8e91962ed7f978e9f640
    ->T(d) 234.7m/3.9h/0.16d, B: 0xe7.. $28.2204(0.0) 0x6dbc40b535e0ab2a1c51704ae49923de9213bf764e9f73ab1d1e00c53e716cc1
    ->T(d) 240.5m/4.0h/0.17d, B: 0xf5.. $28.2204(0.0) 0xf511d8c2495eb7742baa889855fac4978e6d7a3c63075ed9731aad0367013c45
    ->T(d) 234.4m/3.9h/0.16d, B: 0xa2.. $28.395(0.1745999999999981) 0xf67f9e268808126b1947098691dfa6e2e9caf2e686bccd4c54da75534f0be913
    
    0x87b77fabfeb869150b8e1b9462603f9a639c5fae
       T(d) 0.0m/0.0h/0.00d, B: 0x87.. $16.0497(0.0) 0x4ec62340f8ef4a1d92023957acbfbcceb8762108228656bc1793ef6b2a7cfad8
    ->T(d) 240.5m/4.0h/0.17d, B: 0xa2.. $16.0497(0.0) 0xe11dd97c6d84c9b71f6bba81073ebf8dcb05067c1a6974c4f63016721947d4d1
    ->T(d) 235.4m/3.9h/0.16d, B: 0x83.. $16.82133(0.7716299999999983) 0x6684083b83b737c1ec467904ef825a6c8e77e169b9cc06fd91cbb675f7bf3c52
    ->T(d) 240.7m/4.0h/0.17d, B: 0xba.. $16.82133(0.7716299999999983) 0x6abeb239e4020fd9cf3361ceed28c3961cf25b373882d69b0d4ba4dcab5bf6f8
    ->T(d) 234.8m/3.9h/0.16d, B: 0x23.. $16.82133(0.7716299999999983) 0x97554fd9f0ad9660b583d245510a6f4bbd6adff81355b0b316d57e7cdc1c4d5d
    ->T(d) 474.3m/7.9h/0.33d, B: 0x23.. $16.82133(0.7716299999999983) 0x38382d0cb854fd4cdcb35ff2a46a95fc58f3a208ef67e0eb4eed8731c4b40e91
    ->T(d) 240.2m/4.0h/0.17d, B: 0x1c.. $16.82133(0.7716299999999983) 0xa0c9433a00a55595cbf05e3fc26997babb8a472ec8a0c13b47a5dc8051a40743
    ->T(d) 238.3m/4.0h/0.17d, B: 0x23.. $16.945259999999998(0.8955599999999961) 0xa7d5da8b7b990c8afce03cdcd589b2857ec748ba53b8840f5aa4da1560797333
    ->T(d) 238.4m/4.0h/0.17d, B: 0x1c.. $16.945259999999998(0.8955599999999961) 0xbc53475bf9a46f5ccff9777b7fc063cdf8833816db2f08bfb0207d2c793497ee
    ->T(d) 232.4m/3.9h/0.16d, B: 0x00.. $16.945259999999998(0.8955599999999961) 0x8bc7c06580cbb8a1925670f9a808f32e76d8ce651b9f07b0063fc4a1f8a7c6fb
    ->T(d) 239.9m/4.0h/0.17d, B: 0x73.. $16.945259999999998(0.8955599999999961) 0x0ce51445eb9541b23572522d3b215d68dad2c69f28f181f17759521bc455dc2d
    ->T(d) 475.1m/7.9h/0.33d, B: 0x73.. $16.945259999999998(0.8955599999999961) 0x0f8dd00ba2021fb0f72aaf5ac7d944a9a8866b0e8fdac697ebf1e19f66ae0a91
    ->T(d) 234.9m/3.9h/0.16d, B: 0xcd.. $16.945259999999998(0.8955599999999961) 0xb0f0fd376c502cdeea1284b8b7bffe6d68b392e0df7c6c8982d8db746e88468b
    ->T(d) 1190.7m/19.8h/0.83d, B: 0x73.. $17.637330000000006(1.5876300000000043) 0x40152ff53bdb1b7b8f0db3d189173d79c4a2b94b5fdd5d1a4cba711580984487
    ->T(d) 235.5m/3.9h/0.16d, B: 0xcd.. $17.637330000000006(1.5876300000000043) 0x398bfa094df8ebfbcdf49db470cf7a52a98631b969e21f29b2eab3d532a4b3cd
    ->T(d) 240.2m/4.0h/0.17d, B: 0x6e.. $18.5895(2.5397999999999996) 0x98e5775c719265eee24f4c34875f0823a1e8f84e198f4463ec34120de59f45be
    ->T(d) 233.9m/3.9h/0.16d, B: 0x6a.. $18.5895(2.5397999999999996) 0xffa31f615b7d0420e57924412d5cccedffcf4dcc8364a339c5df9dbb306028e2
    ->T(d) 715.9m/11.9h/0.50d, B: 0xba.. $18.5895(2.5397999999999996) 0x57f8e8e53768c7e9f8fa49ed3d079639faa59f6a8a11e430a27cedec0b86b1a7
    ->T(d) 234.4m/3.9h/0.16d, B: 0x6a.. $18.5895(2.5397999999999996) 0x21269fc694de7dfca655d7a57301ee21f6f1d1c98dd5a2a169d428958afe61a7
    ->T(d) 240.8m/4.0h/0.17d, B: 0x6e.. $18.083070000000006(2.033370000000005) 0xf2c67a90f26c905565267de11ac36324a4f8f2c20d1021095ea0b2d35784100c
    ->T(d) 708.8m/11.8h/0.49d, B: 0xe7.. $18.083070000000006(2.033370000000005) 0x7f377a304e535a2bd05ad8fdb7aaa472d546fdd8b7f77355b2e8d15b60df6013
    ->T(d) 240.6m/4.0h/0.17d, B: 0x6e.. $18.083070000000006(2.033370000000005) 0xe4200ca16ee1847685569c802d58b083d5c422acd00947194a891c9e640ce35d
    ->T(d) 234.1m/3.9h/0.16d, B: 0xa3.. $18.083070000000006(2.033370000000005) 0x39f23476de15ff30905f465a9c7ac0b9147912e156de8ae075540bbfc912c820
    ->T(d) 241.3m/4.0h/0.17d, B: 0xf5.. $18.32277(2.273069999999997) 0x5803b38ce16748a669ed80117e745f4da90553b77f165b13a18123819d308777
    ->T(d) 234.0m/3.9h/0.16d, B: 0xa3.. $18.32277(2.273069999999997) 0xba46063891ebbfe617e8c3a1fec173237ee03f2409e126e6a5ec5a2f0d611e7e
    ->T(d) 239.6m/4.0h/0.17d, B: 0xba.. $18.32277(2.273069999999997) 0xd531d85e338b8d8408618131b25401b3f6a419d7c7d3320c92138da4178daeaf
    ->T(d) 260.6m/4.3h/0.18d, B: 0x46.. $13.472625(-2.5770750000000007) 0x76db9401c0a0e100af428de82732997dd7fbccb4c0c58b44afd4df5a091ff99a
    ->T(d) 1165.5m/19.4h/0.81d, B: 0x73.. $16.887629999999998(0.8379299999999965) 0x209e771de7b3a40b3d44e0b725ab04d4904c4f1f7d9e91015966afbcfddaca81
    ->T(d) 7272.5m/121.2h/5.05d, B: 0xba.. $17.030939999999998(0.9812399999999961) 0xe777d15ad372170c99c3318beb76d0409d94462e7713c11d0b4606863e24af7b
    ->T(d) 86.7m/1.4h/0.06d, B: 0x1c.. $17.030939999999998(0.9812399999999961) 0xe8a8245852961f130c79bdeb712bfb42347c08c7d74d7be5cdfca52a344e5f21
    ->T(d) 243.1m/4.1h/0.17d, B: 0xa2.. $17.030939999999998(0.9812399999999961) 0xe871967315e55dae07cb904b9cc36d1c2456f4b704ef163570e68c76f9bbefcd
    ->T(d) 232.9m/3.9h/0.16d, B: 0xba.. $17.030939999999998(0.9812399999999961) 0x483f2e7bc0a0d447459283872d56db784c8ea0d9ceddc0ec6c4465ae8ff5cbd5
    ->T(d) 240.0m/4.0h/0.17d, B: 0x68.. $16.992179999999998(0.9424799999999962) 0xd66b253aa57e8dbab82832c76757ff4f22f3e954591e9923536d14ebc741da78
    ->T(d) 234.1m/3.9h/0.16d, B: 0xcd.. $16.992179999999998(0.9424799999999962) 0x083d74f4201ecc5a954423a2b716b60fa207e5d021bd71e17ab6e6f9c4c04b00
    ->T(d) 240.1m/4.0h/0.17d, B: 0x46.. $16.992179999999998(0.9424799999999962) 0x14a0b96e378f60e75a217ae3558072e59e2ac5a1157236d4d4a72a51a7e79670
    ->T(d) 244.7m/4.1h/0.17d, B: 0xba.. $16.992179999999998(0.9424799999999962) 0xebd9b9923f4d443cf4788401ea9c26fa7690af2d615738372f97bce09709cd73
    ->T(d) 230.0m/3.8h/0.16d, B: 0x6e.. $16.992179999999998(0.9424799999999962) 0x33197a64dbb893d420dae56b3bb34f250c4d97af891229ef3935fd6e9d7f0e8e
    ->T(d) 235.0m/3.9h/0.16d, B: 0x17.. $16.992179999999998(0.9424799999999962) 0xa230372dc57f722a9bc2b51305c34efef997b358c5f7a6d3f19bda960c772985
    ->T(d) 720.2m/12.0h/0.50d, B: 0x6e.. $17.54349(1.493789999999997) 0x329fc6fa896c7ef875fe715a4ebf4772056eb9e077691430ae6d2a365652626b
    ->T(d) 235.2m/3.9h/0.16d, B: 0x17.. $17.54349(1.493789999999997) 0x85e69c08d8b455de1106cf9c0e3875426feb0b5446233f274106b5f79a5b57a2
    ->T(d) 237.1m/4.0h/0.16d, B: 0xe7.. $17.54349(1.493789999999997) 0xb86b22edcda19cbfe7fb82b5a30a889bc28c29725d9591980f4ec174d57efefc
    ->T(d) 232.6m/3.9h/0.16d, B: 0xad.. $17.54349(1.493789999999997) 0x6c0d09db91d0f9450e0a970dc27b590e18e30bdfe7d7a6a4e934efc2bd225281
    ->T(d) 240.0m/4.0h/0.17d, B: 0x83.. $17.70873(1.6590299999999978) 0xc76cf0c6d06ef4e564620bf7300b57fb34cea81fcff47873390a1eb24d6fc048
    ->T(d) 235.7m/3.9h/0.16d, B: 0xf5.. $17.70873(1.6590299999999978) 0x776222b9b742afbaa653acfcb815468c6af33bed36f3d621dc0b9e6980049509
    ->T(d) 239.8m/4.0h/0.17d, B: 0x83.. $17.70873(1.6590299999999978) 0x4f79224ad14098e3b81bdb11fc6200ca274e9504eca7399469f09db46ddee0d0
    ->T(d) 234.7m/3.9h/0.16d, B: 0xe7.. $17.70873(1.6590299999999978) 0x2a06f098f80ad6d136d3e61655799ac64d998d2065bd77e8ba06afb6a863e51b
    ->T(d) 240.1m/4.0h/0.17d, B: 0x1c.. $17.70873(1.6590299999999978) 0xb74cd170fdcb6919fd2e4670c0bd39443663be4684a7a68575d2b90f59395432
    ->T(d) 234.7m/3.9h/0.16d, B: 0x1e.. $17.70873(1.6590299999999978) 0x736cdad38456ab79b31be39215459aa3a86d7339a989ef943ea6a844ea191a8d
    ->T(d) 240.1m/4.0h/0.17d, B: 0x73.. $17.7786(1.7288999999999994) 0x4b36a470de3f674bf5f72abd060a0ee7139b980f08570ca6a706c76494ac4450
    ->T(d) 235.3m/3.9h/0.16d, B: 0x6e.. $17.7786(1.7288999999999994) 0x5a445f37b0ecbd253713e3f7c4756e5975337052f45193191a090b7feb2b1952
    ->T(d) 239.4m/4.0h/0.17d, B: 0xa3.. $17.7786(1.7288999999999994) 0xf1c28633fdc331bd87e4d0dfc17bae09e13a37437be4afef12ec47a8a7897c31
    ->T(d) 235.1m/3.9h/0.16d, B: 0xba.. $17.7786(1.7288999999999994) 0xf4a43ac8badf6696ea368dc2caec73ad5e5d5f5166aec8374a1369c584f25edb
    ->T(d) 240.3m/4.0h/0.17d, B: 0x68.. $17.7786(1.7288999999999994) 0x9c433a337ae4c5eb889fbabcd6d974fc6e272146f579e6ef87d6bdd1eb5d4954
    ->T(d) 234.8m/3.9h/0.16d, B: 0x87.. $17.7786(1.7288999999999994) 0xeae438f153d1b360c5b71f7c6252b86e76e994c7b8faac17feab94b8271bf1c3
    
    0x463215edb66fb6a8f0c979e739a731977617699f
       T(d) 0.0m/0.0h/0.00d, B: 0x46.. $13.472625(0.0) 0x76db9401c0a0e100af428de82732997dd7fbccb4c0c58b44afd4df5a091ff99a
    ->T(d) 1165.5m/19.4h/0.81d, B: 0x73.. $16.887629999999998(3.415004999999997) 0x209e771de7b3a40b3d44e0b725ab04d4904c4f1f7d9e91015966afbcfddaca81
    ->T(d) 7272.5m/121.2h/5.05d, B: 0xba.. $17.030939999999998(3.558314999999997) 0xe777d15ad372170c99c3318beb76d0409d94462e7713c11d0b4606863e24af7b
    ->T(d) 86.7m/1.4h/0.06d, B: 0x1c.. $17.030939999999998(3.558314999999997) 0xe8a8245852961f130c79bdeb712bfb42347c08c7d74d7be5cdfca52a344e5f21
    ->T(d) 243.1m/4.1h/0.17d, B: 0xa2.. $17.030939999999998(3.558314999999997) 0xe871967315e55dae07cb904b9cc36d1c2456f4b704ef163570e68c76f9bbefcd
    ->T(d) 232.9m/3.9h/0.16d, B: 0xba.. $17.030939999999998(3.558314999999997) 0x483f2e7bc0a0d447459283872d56db784c8ea0d9ceddc0ec6c4465ae8ff5cbd5
    ->T(d) 240.0m/4.0h/0.17d, B: 0x68.. $16.992179999999998(3.519554999999997) 0xd66b253aa57e8dbab82832c76757ff4f22f3e954591e9923536d14ebc741da78
    ->T(d) 234.1m/3.9h/0.16d, B: 0xcd.. $16.992179999999998(3.519554999999997) 0x083d74f4201ecc5a954423a2b716b60fa207e5d021bd71e17ab6e6f9c4c04b00
    ->T(d) 240.1m/4.0h/0.17d, B: 0x46.. $16.992179999999998(3.519554999999997) 0x14a0b96e378f60e75a217ae3558072e59e2ac5a1157236d4d4a72a51a7e79670
    
    0x68d31cb3825e559b1e5c0665f2d65d06a17fce1a
       T(d) 0.0m/0.0h/0.00d, B: 0x68.. $13.62671(0.0) 0x7e1c2ce981d4a845e78d5446e23bf9ec7b1365db3c2357a2ec419ba59ce1fb68
    ->T(d) 235.5m/3.9h/0.16d, B: 0xba.. $13.62671(0.0) 0x3968c33d8389d0c823f98746ce9f2171ab765b36cb866c9e91e4e9cabc5bdc74
    ->T(d) 239.3m/4.0h/0.17d, B: 0x87.. $13.62671(0.0) 0xe58cb8b8a98f510de96732937d9ea26dfdcccd9def03afcb7d7b2fe90b0ecf08
    ->T(d) 236.7m/3.9h/0.16d, B: 0xa2.. $13.62671(0.0) 0x7781ca1d0a0297e0dfb316ed50a59acaf11ac1b73e9eeaf85c2ed2dd3f7ff230
    ->T(d) 293.0m/4.9h/0.20d, B: 0x87.. $13.62671(0.0) 0xc152a93dbd281928cdab4c3cfed08964c781a7bee983cac90f2581fb10436938
    ->T(d) 186.7m/3.1h/0.13d, B: 0x46.. $13.62671(0.0) 0x21047ce67a8f331174daddf0206a0a1a303e549d5f72c867774f951d2c019c29
    ->T(d) 233.9m/3.9h/0.16d, B: 0x87.. $13.610910000000002(-0.015799999999996928) 0x481de2ba1fc9f4a23da3c85d5b01cca8e352b91edb00ae3d6349a2d38e937973
    ->T(d) 238.7m/4.0h/0.17d, B: 0x6a.. $13.610910000000002(-0.015799999999996928) 0x46bec14decf77a31c49a35288df80abc4520080bbfd4b33bd5f4c642c83b2019
    ->T(d) 236.5m/3.9h/0.16d, B: 0xf5.. $13.610910000000002(-0.015799999999996928) 0xd6070162feb3cfb491398c747b4340467fd5b3c7e7d08c871ef21fc68608fe82
    ->T(d) 236.9m/3.9h/0.16d, B: 0xad.. $13.610910000000002(-0.015799999999996928) 0x89a5b7a3399d15c358a107287bb3a1a3765aacc2a6adf3af42a59fb3a7968e53
    ->T(d) 237.8m/4.0h/0.17d, B: 0x83.. $13.610910000000002(-0.015799999999996928) 0xdcaf6639f47e893aca26f77065882273daad2428371a199d0c8548b3f1e74d9d
    ->T(d) 235.9m/3.9h/0.16d, B: 0x1e.. $13.610910000000002(-0.015799999999996928) 0x4048ef3b6597f2a680b9303bd4e50a8fc8f5fbbf2a89d05a3fb9d22b4d97d283
    ->T(d) 240.2m/4.0h/0.17d, B: 0x68.. $13.833295(0.20658500000000046) 0x4e328826be388a5d90412957ec8d65eed83a543670f025a7113ea1a660b181f1
    
    0x1c1ef71445010114f41ac1feb32dbf5d7281e90f
       T(d) 0.0m/0.0h/0.00d, B: 0x1c.. $31.47000000000001(0.0) 0x304244be319fa5355f2840fb0c43156f9a3e60d6cba90406726c101b9e266f30
    ->T(d) 239.8m/4.0h/0.17d, B: 0xe7.. $31.47000000000001(0.0) 0x20aa659894463c3f4397e1efa3ede7f133e9cfa060036a4ca1d7c9b611c35456
    ->T(d) 236.8m/3.9h/0.16d, B: 0x1e.. $31.47000000000001(0.0) 0xcdbbaa410dfe1ab77ddb6dcb1f6628d756660e8d7fcb33a2c945bdbc27bf03e3
    ->T(d) 240.1m/4.0h/0.17d, B: 0x46.. $32.983000000000004(1.5129999999999946) 0x3dfa339d95f8d8025938580fda80efae95ad8f51edd23b723194ee44bb2c4517
    ->T(d) 235.0m/3.9h/0.16d, B: 0x8a.. $32.983000000000004(1.5129999999999946) 0x7c71fe21ce497caed5e698c41a76c66e1dca236c461537b4236816da0652b354
    ->T(d) 239.1m/4.0h/0.17d, B: 0x1c.. $32.983000000000004(1.5129999999999946) 0xb6d65630bca094900c3826a84201505d788be9c8f485bf129af0f98cbcd69c4c
    
    0x6e13c7e25c2cda6f5c8c4e431bee480bfb312c28
       T(d) 0.0m/0.0h/0.00d, B: 0x6e.. $20.52341(0.0) 0xd7b31c8f4497dea4da94f2a67959baa475161574ca180f20dda07043c6a7a3c6
    ->T(d) 238.9m/4.0h/0.17d, B: 0x73.. $20.52341(0.0) 0x49fe93ee4b8bb021b8860fc81d83faff7a6d9f7156ac93f95d5e08dc58e273e7
    ->T(d) 235.9m/3.9h/0.16d, B: 0x8a.. $20.52341(0.0) 0xd99ca5729833e66ad1e0eec68dcb53aff71b5273cc7461440e903961ac7ac160
    ->T(d) 1618.0m/27.0h/1.12d, B: 0xba.. $10.414365(-10.109044999999998) 0x6896ef2c720efa3d5116113c7efb168eb37cf79fa584e0db9a3aa451d295b049
    ->T(d) 281.6m/4.7h/0.20d, B: 0x46.. $21.095765000000004(0.5723550000000053) 0x29388ba3e0b31e6ca38525954d1d3b22da26ff510343af8bea68865374234ef5
    ->T(d) 239.9m/4.0h/0.17d, B: 0xba.. $21.095765000000004(0.5723550000000053) 0xe00c306761746ca9a91c37c065022a398094a2e18c567f0e7ffbd6b9fd163f62
    ->T(d) 235.1m/3.9h/0.16d, B: 0xcd.. $21.3537(0.8302900000000015) 0x447cfbdaf516264458f174ab142ae089d1437dba3e57773c64a03ab7d834d09c
    ->T(d) 239.5m/4.0h/0.17d, B: 0xad.. $21.3537(0.8302900000000015) 0x90dddcc3af2976f97df47ab9447366ad57ba3a5c9f2f68cf9045a0b0745b6af4
    ->T(d) 235.1m/3.9h/0.16d, B: 0xe7.. $21.3537(0.8302900000000015) 0x50f200b227746c4934c888133cb43ce6c6a183b2ff9c6841d2c87d60a5d5111e
    ->T(d) 240.1m/4.0h/0.17d, B: 0xa2.. $21.3537(0.8302900000000015) 0x8f8f188c3adfe65957decf363e125298610c64e3882b1467d8c0ab590ad9aee3
    ->T(d) 235.7m/3.9h/0.16d, B: 0x6a.. $21.3537(0.8302900000000015) 0x7c0c1920a6095180cf6f769827963f3845b7ed743e97fde324858fbcc8f76152
    ->T(d) 240.0m/4.0h/0.17d, B: 0x83.. $21.3537(0.8302900000000015) 0x2a81e24dea926722a052bbab633e4ca78a6f1b62de1d233c260627431d69f7d5
    ->T(d) 234.4m/3.9h/0.16d, B: 0x68.. $21.21703(0.6936200000000028) 0x5a407b3b76895a1317dcba9e4b088968d748bfdc888e4d5fa47bece6eaaba580
    ->T(d) 240.5m/4.0h/0.17d, B: 0xa2.. $21.21703(0.6936200000000028) 0xc04e4f03a7ce8b0f94b69ee7ab07b06ba488a1a00ac5873c7a092943b9374d38
    ->T(d) 234.4m/3.9h/0.16d, B: 0x8a.. $21.21703(0.6936200000000028) 0xa4eec091900dfb03b90d61af0f763b9bc9417e71e52280fdf1000b9122a31f77
    ->T(d) 240.6m/4.0h/0.17d, B: 0x00.. $21.21703(0.6936200000000028) 0x6155eba7502a913f5ec581c6610cfdbb5c30121eab822e48f4210ddad36e1734
    ->T(d) 244.6m/4.1h/0.17d, B: 0x23.. $21.21703(0.6936200000000028) 0xe044bd9d2ba1cf27c9770fbe51fe611448c125f8b819df7a29880a15a617e5d2
    ->T(d) 230.6m/3.8h/0.16d, B: 0x17.. $21.21703(0.6936200000000028) 0x73db0534a88420da80b2b7666eed56549a8df78a1f95a6b485c33b124988e982
    ->T(d) 234.8m/3.9h/0.16d, B: 0x1e.. $21.775955(1.2525450000000014) 0x9cee3f5d4a7eb40475c88fdf3c752cfb6526c707c9e431ac89b3c2e4666c56f3
    ->T(d) 240.3m/4.0h/0.17d, B: 0x1c.. $21.775955(1.2525450000000014) 0xd0324f547604964a8b03d8de7e5e951910e0b55edb655acbdbaff21bab637798
    ->T(d) 234.3m/3.9h/0.16d, B: 0xa3.. $21.775955(1.2525450000000014) 0x474a152ecc177d2e1b0defef6b6a765015b7b4979b3d1ca6a2af0da56f5370bc
    ->T(d) 240.2m/4.0h/0.17d, B: 0xe7.. $21.775955(1.2525450000000014) 0x506654fcc5fb67d7e1e4d90de050649cfb38851962e4b7704f2cc017983fc5be
    ->T(d) 235.1m/3.9h/0.16d, B: 0x87.. $21.775955(1.2525450000000014) 0x0967bf15c392b68913361a00d52228d406016c23f03d802307584ce9790ece08
    ->T(d) 241.2m/4.0h/0.17d, B: 0x6e.. $21.775955(1.2525450000000014) 0x7e30311ccd386d19001a2df3f1bf058c340f0cb22cbd79331eaa77cd167e2243
    

All of these traders also follow the same, or a similar pattern.

Trading with each other, waiting ~4 hours before trading again. 

We have completed the example and presented ways to find cycles in NFTs!