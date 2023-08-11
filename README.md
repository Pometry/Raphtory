git co<br>
<p align="center">
  <img src="https://user-images.githubusercontent.com/6665739/130641943-fa7fcdb8-a0e7-4aa4-863f-3df61b5de775.png" alt="Raphtory" height="100"/>
</p>
<p align="center">
</p>

<p align="center">
<a href="https://github.com/Raphtory/Raphtory/actions/workflows/test.yml/badge.svg">
<img alt="Test and Build" src="https://github.com/Raphtory/Raphtory/actions/workflows/test.yml/badge.svg" />
</a>
<a href="https://github.com/Raphtory/Raphtory/releases">
<img alt="Latest Release" src="https://img.shields.io/github/v/release/Raphtory/Raphtory?color=brightgreen&include_prereleases" />
</a>
<a href="https://github.com/Raphtory/Raphtory/issues">
<img alt="Issues" src="https://img.shields.io/github/issues/Raphtory/Raphtory?color=brightgreen" />
</a>
<a href="https://crates.io/crates/raphtory">
<img alt="Crates.io" src="https://img.shields.io/crates/v/raphtory">
</a>
<a href="https://pypi.org/project/raphtory/">
<img alt="PyPI" src="https://img.shields.io/pypi/v/raphtory">
</a>

<a href="https://mybinder.org/v2/gh/Raphtory/Raphtory/master?labpath=examples%2Fpy%2Flotr%2Flotr.ipynb">
<img alt="Launch Notebook" src="https://mybinder.org/badge_logo.svg" />
</a>
</p>
<p align="center">
<a href="https://www.raphtory.com">🌍 Website </a>
&nbsp
<a href="https://docs.raphtory.com/">📒 Documentation</a>
&nbsp 
<a href="https://www.pometry.com"><img src="https://user-images.githubusercontent.com/6665739/202438989-2859f8b8-30fb-4402-820a-563049e1fdb3.png" height="20" align="center"/> Pometry</a> 
&nbsp
<a href="https://docs.raphtory.com/en/master/Introduction/ingestion.html">🧙🏻‍ Tutorial</a> 
&nbsp
<a href="https://github.com/Raphtory/Raphtory/issues">🐛 Report a Bug</a> 
&nbsp
<a href="https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA"><img src="https://user-images.githubusercontent.com/6665739/154071628-a55fb5f9-6994-4dcf-be03-401afc7d9ee0.png" height="20" align="center"/> Join Slack</a> 
</p>

<br>

Raphtory is an in-memory graph tool written in Rust with friendly Python APIs on top. It is blazingly fast, scales to hundreds of millions of edges 
on your laptop, and can be dropped into your existing pipelines with a simple `pip install raphtory`.  

It supports time traveling, multilayer modelling, and advanced analytics beyond simple querying like community evolution, dynamic scoring, and mining temporal motifs.

If you wish to contribute, check out the open [list of issues](https://github.com/Pometry/Raphtory/issues), [bounty board](https://github.com/Raphtory/Raphtory/discussions/categories/bounty-board) or hit us up directly on [slack](https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA). Successful contributions will be reward with swizzling swag!


## Running a basic example

```python
from raphtory import Graph
from raphtory import algorithms as algo
import pandas as pd

# Create a new graph
graph = Graph()

# Add some data to your graph
graph.add_vertex(timestamp=1, id="Alice")
graph.add_vertex(timestamp=1, id="Bob")
graph.add_vertex(timestamp=1, id="Charlie")
graph.add_edge  (timestamp=2, src="Bob",   dst="Charlie", properties={"weight":5.0})
graph.add_edge  (timestamp=3, src="Alice", dst="Bob",     properties={"weight":10.0})
graph.add_edge  (timestamp=3, src="Bob",   dst="Charlie", properties={"weight":-15.0})

# Check the number of unique nodes/edges in the graph and earliest/latest time seen.
print(graph)

results = [["earliest_time", "name", "out_degree", "in_degree"]]

# Collect some simple vertex metrics Ran across the history of your graph with a rolling window
for graph_view in graph.rolling(window=1):
    for v in graph_view.vertices():
        results.append([graph_view.earliest_time(), v.name(), v.out_degree(), v.in_degree()])

# Print the results
print(pd.DataFrame(results[1:], columns=results[0]))

# Grab an edge, explore the history of its 'weight' 
cb_edge = graph.edge("Bob","Charlie")
weight_history = cb_edge.properties.temporal.get("weight").items()
print("The edge between Bob and Charlie has the following weight history:", weight_history)

# Compare this weight between time 2 and time 3
weight_change = cb_edge.at(2)["weight"] - cb_edge.at(3)["weight"]
print("The weight of the edge between Bob and Charlie has changed by",weight_change,"pts")

# Run pagerank and ask for the top ranked node
top_node = algo.pagerank(graph).top_k(1)
print("The most important node in the graph is",top_node[0][0],"with a score of",top_node[0][1])
```

```a
Graph(number_of_edges=2, number_of_vertices=3, earliest_time=1, latest_time=3)

|   | earliest_time | name    | out_degree | in_degree |
|---|---------------|---------|------------|-----------|
| 0 | 1             | Alice   | 0          | 0         |
| 1 | 1             | Bob     | 0          | 0         |
| 2 | 1             | Charlie | 0          | 0         |
| 3 | 2             | Bob     | 1          | 0         |
| 4 | 2             | Charlie | 0          | 1         |
| 5 | 3             | Alice   | 1          | 0         |
| 6 | 3             | Bob     | 1          | 1         |
| 7 | 3             | Charlie | 0          | 1         |

The edge between Bob and Charlie has the following weight history: [(2, 5.0), (3, -15.0)]

The weight of the edge between Bob and Charlie has changed by 20.0 pts

The top node in the graph is Charlie with a score of 0.4744116163405977
```

## GraphQL

### Create/Load a graph

Save a raphtory graph and set the `GRAPH_DIRECTORY` environment variable to point to the directory containing the graph.

<details>

<summary> 
Alternatively you can run the code below to generate a graph.
</summary>

```bash
mkdir -p /tmp/graphs
mkdir -p examples/rust/src/bin/lotr/data/
tail -n +2 resource/lotr.csv > examples/rust/src/bin/lotr/data/lotr.csv

cd examples/rust && cargo run --bin lotr -r

cp examples/rust/src/bin/lotr/data/graphdb.bincode /tmp/graphs/lotr.bincode
```

</details>


### Run the GraphQL server

The code below will run GraphQL with a UI at `localhost:1736` 

GraphlQL will look for graph files in `/tmp/graphs` or in the path set in the `GRAPH_DIRECTORY` Environment variable. 

```bash
cd raphtory-graphql && cargo run -r 
```

<details>
<summary>ℹ️Warning: Server must have the same version + environment</summary>
The GraphQL server must be running in the same environment (i.e. debug or release) and same Raphtory version as the generated graph, otherwise it will throw errors due to incompatible graph metadata across versions. 
</details>

<details>
<summary>Following will be output upon a successful launch</summary>

```bash
warning: `raphtory` (lib) generated 17 warnings (run `cargo fix --lib -p raphtory` to apply 13 suggestions)
    Finished release [optimized] target(s) in 0.91s
     Running `Raphtory/target/release/raphtory-graphql`
loading graph from /tmp/graphs/lotr.bincode
Playground: http://localhost:1736
  2023-08-11T14:36:52.444203Z  INFO poem::server: listening, addr: socket://0.0.0.0:1736
    at /Users/pometry/.cargo/registry/src/github.com-1ecc6299db9ec823/poem-1.3.56/src/server.rs:109

  2023-08-11T14:36:52.444257Z  INFO poem::server: server started
    at /Users/pometry/.cargo/registry/src/github.com-1ecc6299db9ec823/poem-1.3.56/src/server.rs:111
```
</details>


### Execute a query

Go to the Playground at `http://localhost:1736` and execute the following commands:

Query:
```bash
    query GetNodes($graphName: String!) {
        graph(name: $graphName) {
            nodes {
              name
            }
      }
    }
```

Query Variables:
```bash
{
  "graphName": "lotr.bincode"
}
```

Expected Result:
```bash
{
  "data": {
    "graph": {
      "nodes": [
        {
          "name": "Gandalf"
        },
        {
          "name": "Elrond"
        },
        {
          "name": "Frodo"
        },
        {
          "name": "Bilbo"
        },
        ...
```




## Installing Raphtory 

Raphtory is available for Python and Rust as of version 0.3.0. You should have Python version 3.10 or higher and it's a good idea to use conda, virtualenv, or pyenv. 

```bash
pip install raphtory
``` 

## Examples and Notebooks

Check out Raphtory in action with our interactive Jupyter Notebook! Just click the badge below to launch a Raphtory sandbox online, no installation needed.

 [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/Raphtory/Raphtory/master?labpath=examples%2Fpy%2Flotr%2Flotr.ipynb) 

Want to give Raphtory a go on your laptop? You can checkout out the [latest documentation](https://docs.raphtory.com/) and [complete list of available algorithms](https://docs.raphtory.com/en/v0.2.0/api/_autosummary/raphtory.algorithms.html) or hop on our notebook based tutorials below!


#### Getting started

| Type     | Description                                                                              |
|----------|------------------------------------------------------------------------------------------|
| Tutorial | [Building your first graph](https://docs.raphtory.com/en/master/Introduction/ingestion.html) |

#### Developing an end-to-end application

| Type | Description                                                                                                                                                   |
| ------------- |---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Notebook | [Use our powerful time APIs to find pump and dump scams in popular NFTs](https://github.com/Raphtory/Raphtory/blob/master/examples/py/nft/nft_analysis.ipynb) |


# Benchmarks

We host a page which triggers and saves the result of two benchmarks upon every push to the master branch. 

View this here [https://pometry.github.io/Raphtory/dev/bench/](https://pometry.github.io/Raphtory/dev/bench/)

# Bounty board

Raphtory is currently offering rewards for contributions, such as new features or algorithms. Contributors will receive swag and prizes! 

To get started, check out our list of desired algorithms at https://github.com/Raphtory/Raphtory/discussions/categories/bounty-board which include some low hanging fruit (🍇) that are easy to implement. 


# Community  

Join the growing community of open-source enthusiasts using Raphtory to power their graph analysis projects!

- Follow [![Slack](https://img.shields.io/twitter/follow/raphtory?label=@raphtory)](https://twitter.com/raphtory) for the latest Raphtory news and development

- Join our [![Slack](https://img.shields.io/badge/community-Slack-red)](https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA) to chat with us and get answers to your questions!


## Contributors

<a href="https://github.com/raphtory/raphtory/graphs/contributors"><img src="https://contrib.rocks/image?repo=raphtory/raphtory"/></a>

Want to get involved? Please join the Raphtory [Slack](https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA) group and speak with us on how you could pitch in!

## License  

Raphtory is licensed under the terms of the GNU General Public License v3.0 (check out our LICENSE file).



