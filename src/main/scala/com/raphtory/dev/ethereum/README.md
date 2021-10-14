# Ethereum Analytics

`Runner.scala` -  File to run the example

`spout/EthereumTransactionSpout.scala` -  spout used to ingest ethereum transaction data from parquet files

`spout/EthereumGraphBuilder.scala` - reads the tag file, builds the graph from the spout 

## Environment Variables 

`TAG_FILE.csv` - This is a csv containing ethereum tags scraped. 
You can use scrape the tags or obtain an open-source scrape of 
Etherscan tags from `https://github.com/initc3/forsage/raw/master/5-SchemeStatistics/etherscan_tags.csv`
The format is: 

```
address               string
name                  string
family                string
type                  string
website               string
token_market_cap      string
token_num_holders     string
tx_count             float64
balance               string
```


`FILE_SPOUT_DIRECTORY` - This is the folder containing the Ethereum parquet files. 
The files must be seperated into their own fully functional parquets. 
The format is as follows: 

```
hash                         string
nonce                         int64
block_hash                   string
block_number                  int64
transaction_index             int64
from_address                 string
to_address                   string
value                       float64
gas                           int64
gas_price                     int64
block_timestamp               int64
max_fee_per_gas                 int
max_priority_fee_per_gas        int
transaction_type              int64
```

# Algorithms

## Simple taint tracking

This simple taint tracking algorithm will follow the taint a node spreads, following
its transactions and who they infect over time until any stop conditions are met, or
until the graph is complete.

### Parameters

`startTime` - the unix time for when to start the algorithm

`infectedNodes` - a set of nodes to start from (their addresses as lowercase strings)

`stopNodes` - a set of nodes where the infection stops (their addresses as lowercase strings)

### Result

The results are in the following format, as a csv file. 

Time infection was run, address of node infected, status, List of transactions it was infected by. 
The list will contain, the taint status, the transaction hash, time of infection, 
 and the node it was infected by. 

### Use cases

This can be used to follow the spread of, for example, illicit funds within a network. 
For example, lets say at time T1 a Node steals coins. We can mark the thief as infected, 
and then identify which other nodes they spread their coins to. 

Alternatively, it can be used to follow the spread of an infection. Lets say Person A 
was infected at time T1, then they infected person B at time T2. This can then follow
Person B and time T2 onwards, identifying anyone they have infected. 
