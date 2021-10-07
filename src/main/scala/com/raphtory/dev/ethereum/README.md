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

As of 7 Oct 2021, this code and the dev branch of Raphtory ingests at 
approximately 20k transactions a second. 