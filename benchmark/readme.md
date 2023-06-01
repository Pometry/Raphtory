# Benchmarks

This is the raphtory benchmarking suite. 
It is designed to test the performance of raphtory against other graph processing systems.

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
      - 

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

|           | Setup  | Degree | Out Neighbours | Page Rank | Connected Components |
|------------|--------|-------|----------------|-----------|----------------------|
| Raphtory   | 121.04 | 3.90  | 28.69          | 153.22    | 67.6301              |
| GraphTool  | 194.09 | 0.008 | 43.30          | 4.75      | 3.83                 |
| Kuzu       |        |       |                |           |                      |
| NetworkX   |        |       |                |           |                      |
| Neo4J      |        |       |                |           |                      |
| MemGraph   |        |       |                |           |                      |
| Cozo       |        |       |                |           |                      |
