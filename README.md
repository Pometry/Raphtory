<br>
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
<a href="https://pypi.org/project/raphtory/#history">
<img alt="PyPI Downloads" src="https://img.shields.io/pypi/dm/raphtory.svg">
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
<a href="https://www.raphtory.com/user-guide/installation/">🧙Tutorial</a> 
&nbsp
<a href="https://github.com/Raphtory/Raphtory/issues">🐛 Report a Bug</a> 
&nbsp
<a href="https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA"><img src="https://user-images.githubusercontent.com/6665739/154071628-a55fb5f9-6994-4dcf-be03-401afc7d9ee0.png" height="20" align="center"/> Join Slack</a> 
</p>

<br>

Raphtory is an in-memory vectorised graph database written in Rust with friendly Python APIs on top. It is blazingly fast, scales to hundreds of millions of edges 
on your laptop, and can be dropped into your existing pipelines with a simple `pip install raphtory`.  

It supports time traveling, full-text search, multilayer modelling, and advanced analytics beyond simple querying like automatic risk detection, dynamic scoring, and temporal motifs.

If you wish to contribute, check out the open [list of issues](https://github.com/Pometry/Raphtory/issues), [bounty board](https://github.com/Raphtory/Raphtory/discussions/categories/bounty-board) or hit us up directly on [slack](https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA). Successful contributions will be reward with swizzling swag!

## Installing Raphtory

Raphtory is available for Python and Rust. 

For python you must be using version 3.8 or higher and can install via pip:
```bash
pip install raphtory
``` 
For Rust, Raphtory is hosted on [crates](https://crates.io/crates/raphtory) and can be included in your project via `cargo add`:
```bash
cargo add raphtory
```

## Running a basic example
Below is a small example of how Raphtory looks and feels when using our Python APIs. If you like what you see, you can dive into a full tutorial [here](https://www.raphtory.com/user-guide/ingestion/1_creating-a-graph/).
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
graph.add_edge(timestamp=2, src="Bob", dst="Charlie", properties={"weight": 5.0})
graph.add_edge(timestamp=3, src="Alice", dst="Bob", properties={"weight": 10.0})
graph.add_edge(timestamp=3, src="Bob", dst="Charlie", properties={"weight": -15.0})

# Check the number of unique nodes/edges in the graph and earliest/latest time seen.
print(graph)

results = [["earliest_time", "name", "out_degree", "in_degree"]]

# Collect some simple vertex metrics Ran across the history of your graph with a rolling window
for graph_view in graph.rolling(window=1):
    for v in graph_view.vertices:
        results.append(
            [graph_view.earliest_time, v.name, v.out_degree(), v.in_degree()]
        )

# Print the results
print(pd.DataFrame(results[1:], columns=results[0]))

# Grab an edge, explore the history of its 'weight'
cb_edge = graph.edge("Bob", "Charlie")
weight_history = cb_edge.properties.temporal.get("weight").items()
print(
    "The edge between Bob and Charlie has the following weight history:", weight_history
)

# Compare this weight between time 2 and time 3
weight_change = cb_edge.at(2)["weight"] - cb_edge.at(3)["weight"]
print(
    "The weight of the edge between Bob and Charlie has changed by",
    weight_change,
    "pts",
)

# Run pagerank and ask for the top ranked node
top_node = algo.pagerank(graph).top_k(1)
print(
    "The most important node in the graph is",
    top_node[0][0],
    "with a score of",
    top_node[0][1],
)
```
### Output:
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

As part of the python APIs you can host your data within Raphtory's GraphQL server. This makes it super easy to integrate your graphy analytics with web applications.

Below is a small example creating a graph, running a server hosting this data, and directly querying it with our GraphQL client.

```python
from raphtory import Graph
from raphtory import graphqlserver
import pandas as pd

# URL for lord of the rings data from our main tutorial
url = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr-with-header.csv"
df = pd.read_csv(url)

# Load the lord of the rings graph from the dataframe
graph = Graph.load_from_pandas(df,"src_id","dst_id","time")

#Create a dictionary of queryable graphs and start the graphql server with it. This returns a client we can query with
client = graphqlserver.run_server({"lotr_graph":graph},daemon=True)

#Run a basic query to get the names of the characters + their degree
results = client.query("""{
             graph(name: "lotr_graph") {
                 nodes {
                     name
                     degree
                    }
                 }
             }""")

print(results)
```

### Output:
```bash
Loading edges: 100%|██████████████| 2.65K/2.65K [00:00<00:00, 984Kit/s]
Playground: http://localhost:1736
{'graph': 
    {'nodes': 
        [{'name': 'Gandalf', 'degree': 49}, 
         {'name': 'Elrond', 'degree': 32}, 
         {'name': 'Frodo', 'degree': 51}, 
         {'name': 'Bilbo', 'degree': 21}, 
         ...
        ]
    }
}
```
### GraphQL Playground
When you host a Raphtory GraphQL server you get a web playground bundled in, accessible on the same port within your browser (defaulting to 1736). Here you can experiment with queries on your graphs and explore the schema. An example of the playground can be seen below, running the same query as in the python example above.

![GraphQL Playground](https://i.imgur.com/p0HH6v3.png)

## Getting started
To get you up and running with Raphtory we provide a full set of tutorials on the [Raphtory website](https://raphtory.com):

* [Getting Data into Raphtory](https://www.raphtory.com/user-guide/ingestion/1_creating-a-graph/)
* [Basic Graph Queries](https://www.raphtory.com/user-guide/querying/1_intro/)
* [Time Travelling and Graph views](https://www.raphtory.com/user-guide/views/1_intro/)
* [Running algorithms](https://www.raphtory.com/user-guide/algorithms/1_intro/)
* [Integrating with other tools](https://www.raphtory.com/user-guide/algorithms/1_intro/)

If API documentation is more your thing, you can dive straight in [here](https://docs.raphtory.com/)!

### Online notebook sandbox
Want to give this a go, but can't install? Check out Raphtory in action with our interactive Jupyter Notebooks! Just click the badge below to launch a Raphtory sandbox online, no installation needed.

 [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/Raphtory/Raphtory/master?labpath=examples%2Fpy%2Flotr%2Flotr.ipynb) 

## Community

Join the growing community of open-source enthusiasts using Raphtory to power their graph analysis!

- Follow [![Slack](https://img.shields.io/twitter/follow/raphtory?label=@raphtory)](https://twitter.com/raphtory) for the latest Raphtory news and development

- Join our [![Slack](https://img.shields.io/badge/community-Slack-red)](https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA) to chat with us and get answers to your questions!

### Contributors

<a href="https://github.com/raphtory/raphtory/graphs/contributors"><img src="https://contrib.rocks/image?repo=raphtory/raphtory"/></a>

### Bounty board

Raphtory is currently offering rewards for contributions, such as new features or algorithms. Contributors will receive swag and prizes!

To get started, check out our list of [desired algorithms](https://github.com/Raphtory/Raphtory/discussions/categories/bounty-board) which include some low hanging fruit (🍇) that are easy to implement.


## Benchmarks

We host a page which triggers and saves the result of two benchmarks upon every push to the master branch.  View this [here](https://pometry.github.io/Raphtory/dev/bench/)

## License  

Raphtory is licensed under the terms of the GNU General Public License v3.0 (check out our LICENSE file).



