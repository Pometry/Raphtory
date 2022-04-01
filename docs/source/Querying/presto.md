# Querying in-flight data

This example builds a raw data stream that can be queried in real-time with Pulsar SQL.
The code for this example can be found in [here](https://github.com/Raphtory/Examples/tree/0.5.0/raphtory-example-presto)

## What is Pulsar SQL?

Pulsar SQL is a query engine that can be used to run analytic queries on data.
Pulsar has in-built SQL support that allows you to query data directly from Topics.
You can find more information on the [official docs](https://pulsar.apache.org/docs/en/sql-overview/)

### Ethereum Pulsar SQL example 

Here we present a Pulsar SQL demo using the Ethereum dataset.

The data is a `csv` file (comma-separated values) is located in the `resources` folder.
Each line contains an Ethereum Transaction with the following format.

``` 
hash,nonce,block_hash,block_number,transaction_index,from_address,to_address,value,gas,gas_price,input,block_timestamp,max_fee_per_gas,max_priority_fee_per_gas,transaction_type
```

By default, the File Reader in Pulsar parses data into a byte stream.

To query this data with Pulsar SQL we need to convert this into objects recognised with a schema.

In the examples folder you will find `EthereumTransaction.scala`, `EthereumPrestoSpout.scala`
and `Runner.scala`.

* `Runner.scala` runs the application including ingesting the data and mapping it to a schema
* `EthereumPrestoSpout.scala` builds the data stream, convering the CSV into an EthereumTransaction object
* `EthereumTransaction.scala` is the schema that is used to create objects recognised by Presto

   ```scala 
   val spout = new EthereumPrestoSpout[EthereumTransaction](ethSchema, "ethereum_tx.csv")
   RaphtoryGraph[EthereumTransaction](spout)
   ```

The code above shows that this will instead use the EthereumPrestoSpout.
This will now run both the ResourceSpout which will read the Ethereum CSV file
using the File Reader into the `raphtory_data_raw` topic, and then
simultaneously convert this into EthereumTransaction objects into the `eth_data`
topic.

## Usage

1. Create a lib directory and import Raphtory jar.

    ```sh
   $ mkdir lib
   // download the latest from https://github.com/Raphtory/Raphtory/releases/latest
    ```
2. Run the code.

    ```sh
    $ sbt run
    ```

3. In a separate terminal you must run the Pulsar SQL Server

    ```sh
    $ ./pulsar sql-worker run
    ```

4. In another terminal you must run Pulsar SQL.
   This is where we will run queries.

    ```sh
    $ ./pulsar sql
    ```

## Querying

In the Pulsar SQL terminal run

```sh
presto>  USE pulsar."public/default";
```

You are now ready to run queries, for example

```sh
presto> select * from "eth_data" LIMIT 5;
```

Which will return

```
                             block_hash                            | block_number | block_timestamp |                from_address                |  gas   |  gas_price  |                                hash                                | max_fee_per_gas | max_priority_fee_per_gas |  nonce  |                 to_address                 | transaction_index | transsaction_type  |    value     | __partition__ |     __event_time__      |    __publish_time__     | __message_id__ | __sequence_id__ | __producer_name__ |                              __key__                               | __properties__
-------------------------------------------------------------------+--------------+-----------------+--------------------------------------------+--------+-------------+--------------------------------------------------------------------+-----------------+--------------------------+---------+--------------------------------------------+-------------------+--------------------+--------------+---------------+-------------------------+-------------------------+----------------+-----------------+-------------------+--------------------------------------------------------------------+----------------
0xe026a77edcab6103b0f3d25d48dbfd2224979e3e5c047f51758d368e24f2c349 |     10500400 |      1595302464 | 0x3960a3649aa1e985ded1e21cf8d08cfdabbe16ff |  21000 | 88500000000 | 0x0dad6736f7b17d3cfc328e3e768f5f9137de4d739c72e809f8609af552a4683f |                 |                          |       6 | 0x947f707078bbb2d4bfa8a4376c7c73db09e6844a |                 0 |                  0 |      2.42E16 |            -1 | 1970-01-01 01:00:00.000 | 2022-02-02 16:19:38.962 | (12,0,0)       |               0 | standalone-0-42   | 0x0dad6736f7b17d3cfc328e3e768f5f9137de4d739c72e809f8609af552a4683f | {}
0xe026a77edcab6103b0f3d25d48dbfd2224979e3e5c047f51758d368e24f2c349 |     10500400 |      1595302464 | 0x18792b42eba8ae7832ada0c25b9065f8f642c0ab |  42192 | 87000001853 | 0x24f2f8d3bda61a50804cb7722a6e7aff145b4266e7431ae2727beccae0d7d72d |                 |                          |      10 | 0x95a41fb80ca70306e9ecf4e51cea31bd18379c18 |                 1 |                  0 |          0.0 |            -1 | 1970-01-01 01:00:00.000 | 2022-02-02 16:19:38.978 | (12,1,0)       |               1 | standalone-0-42   | 0x24f2f8d3bda61a50804cb7722a6e7aff145b4266e7431ae2727beccae0d7d72d | {}
0xe026a77edcab6103b0f3d25d48dbfd2224979e3e5c047f51758d368e24f2c349 |     10500400 |      1595302464 | 0x1e68f67a8d796a88de11c17f12e6d36c3e81166e |  21000 | 80000000000 | 0x44793ad70294747022de8930db9f0533c014ac3f107185d534ffd7e72187be21 |                 |                          |     264 | 0x1e68f67a8d796a88de11c17f12e6d36c3e81166e |                 2 |                  0 |          0.0 |            -1 | 1970-01-01 01:00:00.000 | 2022-02-02 16:19:38.978 | (12,1,1)       |               2 | standalone-0-42   | 0x44793ad70294747022de8930db9f0533c014ac3f107185d534ffd7e72187be21 | {}
0xe026a77edcab6103b0f3d25d48dbfd2224979e3e5c047f51758d368e24f2c349 |     10500400 |      1595302464 | 0x7598c75ce00fe7a401f46738ec357a6bc73627d2 | 164842 | 80000000000 | 0x8d0ed5528cb216228cffa3896eaf0875de27f97a8d00553aa610b05b884fd9c3 |                 |                          |    2898 | 0x7a250d5630b4cf539739df2c5dacb4c659f2488d |                 3 |                  0 |       5.0E18 |            -1 | 1970-01-01 01:00:00.000 | 2022-02-02 16:19:38.978 | (12,1,2)       |               3 | standalone-0-42   | 0x8d0ed5528cb216228cffa3896eaf0875de27f97a8d00553aa610b05b884fd9c3 | {}
0xe026a77edcab6103b0f3d25d48dbfd2224979e3e5c047f51758d368e24f2c349 |     10500400 |      1595302464 | 0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be |  21000 | 78000000000 | 0x1507bfff000107392c1c918119ce077af3167b3f1a06a3103a40e5abb77f67c6 |                 |                          | 4670579 | 0xa4e411a2b985492f1b9b997fd55ce8fa970290db |                 4 |                  0 | 3.4092908E19 |            -1 | 1970-01-01 01:00:00.000 | 2022-02-02 16:19:38.978 | (12,1,3)       |               4 | standalone-0-42   | 0x1507bfff000107392c1c918119ce077af3167b3f1a06a3103a40e5abb77f67c6 | {}
```

This confirms the objects have been created and can now be used to perform further queries. 

## IntelliJ setup guide

As of 17th Nov. This is a guide to run this within IntelliJ

1. From https://adoptopenjdk.net/index.html download OpenJDK 11 (LTS) with the HotSpot VM
2. Enable this as the project SDK under File > Project Structure > Project Settings > Project > Project SDK
3. Create a new configuration as an `Application` , select Java 11 as the build, and `com.raphtory.examples.lotr.Runner` as the class, add the Environment Variables also


