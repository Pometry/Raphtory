# Benchmarks

This is the raphtory benchmarking suite. 
It is designed to test the performance of raphtory against other graph processing systems.

There are two benchmarks suites, one for python with support for multiple systems, and one for rust with support for 
 raphtory only.


## Rust Suite

This only does a light benchmark of raphtory, as it is designed to be used standalone. 

    Raphtory Quick Benchmark
    
    Usage: raphtory-rust-benchmark [OPTIONS]
    
    Options:
    --header                     Set if the file has a header, default is False
    --delimiter <DELIMITER>      Delimiter of the csv file [default: "\t"]
    --file-path <FILE_PATH>      Path to a csv file [default: ]
    --from-column <FROM_COLUMN>  Position of the from column in the csv [default: 0]
    --to-column <TO_COLUMN>      Position of the to column in the csv [default: 1]
    --time-column <TIME_COLUMN>  Position of the time column in the csv, default will ignore time [default: -1]
    --download                   Download default files
    --debug                      Debug to print more info to the screen
    -h, --help                       Print help
    -V, --version                    Print version


First download the example file by cd'ing into the `raphtory-rust-benchmark` folder and running

    cargo run --release -- --download

This will download the example file into tmp folder on your system, it will give you the file path.

You can then run the benchmark by running, with the file path it has given you

    cargo run --release -- --file-path <file_path>

You can also provide your own file path, but please ensure you have set the correct arguments. 
I.e Whether it has a header, what the delimiter is, and what columns are what.

e.g.

    cargo run --release -- --file-path="/Users/1337/Documents/dev/Data/lotr.csv" --delimiter="," --from-column=0 --to-column=1


## Python Suite

This benchmarks the python version of raphtory.
Please ensure your python environment has raphtory installed. 

Systems currently supported are:
- [raphtory](https://github.com/Pometry/Raphtory)
- [neo4j](https://neo4j.com/)
- [graph-tool](https://graph-tool.skewed.de/)
- [networkx](https://networkx.org/)
- [kuzu](https://kuzudb.com)
- [memgraph](https://memgraph.com/)
- [cozodb](https://github.com/cozodb/cozo)

All benchmarks are run in docker, this documentation ONLY supports docker.
They can be run outside of docker, however this requires you to install all the systems yourself.

These benchmarks run using a slim version of the pokec dataset, which is a social network dataset. 
More information available [here](https://snap.stanford.edu/data/soc-pokec.html)

# Requirements

- [docker](https://docs.docker.com/get-docker/)
- python 3.10
    - python libraries
      - docker
      - requests
      - tqdm
      - pandas
      - raphtory
      - neo4j

# Install if you are not using docker

    pip install networkx scipy matplotlib raphtory kuzu neo4j 

# How to run

1. First download the pokec dataset, this can be done by running below. 
This will download the dataset into a folder called 'data'


    python benchmark_driver.py -b download

2. Benchmarks can be run by running the below command with a benchmark name


    python benchmark_driver.py -b <benchmark_name>

Supported benchmark names are:

- `all` Run All 
- `download` Download Data 
- `r` Run Raphtory Benchmark 
- `gt` Run GraphTool Benchmark 
- `k` Run Kuzu Benchmark 
- `nx` Run NetworkX Benchmark 
- `neo` Run Neo4j Benchmark 
- `mem` Run Memgraph Benchmark 
- `cozo` Run CozoDB Benchmark

You will see the following 

    Welcome to the Raphtory Benchmarking Tool
    Running dockerised benchmarking...
    ** Running dockerized benchmark neo...
    ** Running for Neo4j...
    Starting docker container...
    Creating Docker client...
    Pulling Docker image...
    Defining volumes...
    Running Docker container & benchmark...
    Running command ... 
    ... 
    Welcome to the Raphtory Benchmarking Tool
    Running local benchmarking...
    ** Running for Neo4j...
    ** Running setup...
    setup time: 19.72971820831299
    ** Running degree...
    degree time: 0.30420994758605957
    ** Running out_neighbours...
    out_neighbours time: 0.12140679359436035
    ** Running page_rank...
    page_rank time: 0.6003847122192383
    ** Running connected_components...
    connected_components time: 0.3989684581756592
    setup      degree   out_neighbours  page_rank  connected_components
    Neo4j  19.729718  0.30421  0.121407        0.600385   0.398968
    
    Completed command...
    Benchmark completed, retrieving results...
    Removing container...
    Docker container exited with code 0
    Logs: setup,degree,out_neighbours,page_rank,connected_components
    19.72971820831299,0.30420994758605957,0.12140679359436035,0.6003847122192383,0.3989684581756592

## Alternative args

- To run without docker add the `--no-docker` arg


# Results 

These benchmarks were run on Amazon AWS m5ad.4xlarge instances. 
All the scripts and data were stored on the instance NVME drive.

|           | Setup  | Degree | Out Neighbours | Page Rank | Connected Components |
|-----------|--------|--------|----------------|-----------|----------------------|
| Raphtory  | 110.13 | 2.49   | 23.04          | 1.09      | 17.89                |
| GraphTool | 194.09 | 0.008  | 43.30          | 4.75      | 3.83                 |
| Kuzu      | 18.17  | 1.13   | 89.03          | NOT IMPL  | NOT IMPL             |
| NetworkX  | 130.57 | 1.17   | 24.42          | 162.0     | 160.99               |
| Neo4J     |        |        |                |           |                      |
| MemGraph  | 498.38 | 73.08  | 75.574         | 131.46    | 142.55               |
| Cozo      | 137.82 | 35.36  | 35.17          | 32.83     | N/A SEG FAULT        |

Some key notes:

- Network
  - We compared the results of our pagerank in Raphtory with NetworkX using a directed graph
  and the results are identical. 

- Kuzu
  - Does not support page rank or connected components
  - Out neighbours was run in batches of 100k users at a time, as if you attempt to get all users results it crashes and
  exceeds some buffer size

- Neo4J
  - Due to the way Neo4J imports batch data, the data import was run in offline mode using
    the Neo4j admin tool, this was counted in the setup time. The script then starts a Neo4J
    instance and waits for it to be online, we give the script 50 seconds to do this. These 
    50 seconds are removed from the setup time
  - Due to this, we had to change the format of the data specifically for Neo4J so it would
    import properly. So Neo4J runs a pre-processing step before running the benchmark. This
    alters the data, creating a header and adding labels. This is not counted in the setup time. 
    However, it means the data ingested by neo is ever so larger
  - The admin ingestion also required that we use both a node list and edge list, which we did
    not need for some of the other tools. 

- Memgraph 
  - It was advised to create indexes with the node list prior to relationships, which was done
    in the setup time. This could be why it took the longest to setup.
    https://memgraph.com/docs/memgraph/import-data/load-csv-clause#one-type-of-nodes-and-relationships

- Cozo
  - Triggered a segmentation fault when running the connected components algorithm 
