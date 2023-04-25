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

Raphtory is a powerful analytics engine for large-scale graph analysis. It lets you run complex queries on your data, 
no matter where it's stored or what format it's in. But that's not all - Raphtory's real superpower is its ability to 
track and explore the history of a complex system, from "time traveling" through data to executing advanced analysis 
like taint tracking, temporal reachability, and mining temporal motifs.

**Raphtory is easy to use:** just run a single `pip install raphtory` command and embed it with your existing Python/Pandas pipeline for input and output.

**Raphtory is expressive:** It's designed to represent all types of graph queries and has a well-developed API for exploring your data across its history.

**Raphtory is lightning-fast and scales effortlessly**: Our core is built upon rust. Raphtory can be run on a laptop or a distributed cluster for terabyte-scale graphs.



# Running a basic example

```python
# Import raphtory
from raphtory import Graph

# Create a new local or distributed context
graph = Graph(1)

# Add some data to your graph
graph.add_vertex(1, 1)
graph.add_vertex(2, 2)
graph.add_vertex(3, 3)
graph.add_edge(4, 1, 2)
graph.add_edge(4, 1, 3)

# Collect some simple vertex metrics
# Ran across a range of the data with incremental windowing
graph_set = graph.window(1,5).rolling(1)

results = [["timestamp", "window", "name", "out_degree", "in_degree"]]

for rolling_graph in graph_set:
    for v in rolling_graph.vertices():
        window = rolling_graph.end() - rolling_graph.start()
        results.append([rolling_graph.earliest_time(), window, v.name(), v.out_degree(), v.in_degree()])
    

# Preview DataFrame
pd.DataFrame(results[1:], columns=results[0])
```

```a

|    |   timestamp |   window |   name |   out_degree |   in_degree |
|----|-------------|----------|--------|--------------|-------------|
|  0 |           1 |        1 |      1 |            0 |           0 |
|  1 |           2 |        1 |      2 |            0 |           0 |
|  2 |           3 |        1 |      3 |            0 |           0 |
|  3 |           4 |        1 |      1 |            2 |           0 |
|  4 |           4 |        1 |      2 |            0 |           1 |
|  5 |           4 |        1 |      3 |            0 |           1 |
```

# Installing Raphtory 

Raphtory is available for Python and Rust as of version 0.3.0. We recommend using the raphtory client for Python, which includes everything you need and can be run locally or in distributed mode.

You should have Python version 3.9 or higher. It's a good idea to use conda, virtualenv, or pyenv. 

```bash
pip install raphtory
``` 

# Examples and Notebooks

Check out Raphtory in action with our interactive Jupyter Notebook! Just click the badge below to launch a Raphtory sandbox online, no installation needed.

 [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/Raphtory/Raphtory/master?labpath=examples%2Fpy%2Flotr%2Flotr.ipynb) 

Want to see what Raphtory can do? Scroll down for more.

#### 1. Getting started

| Type | Location                                                                                | Description |
| ------------- |-----------------------------------------------------------------------------------------| ------------- |
| Example | <a href="https://docs.raphtory.com/en/master/Introduction/ingestion.html">ingestion</a> | Loading some sample data into Raphtory |
| Example | <a href="https://docs.raphtory.com/en/v0.0.11/install/python/raphtory.html#Running-your-first-Query">degree count</a>  | Running the simplest graph query in Raphtory|
| Example | <a href="">timetravel (COMING SOON)</a>                                                 | Understanding the time APIs in Raphtory |

#### 2. Running some algorithms 

| Type | Location                                                                                                                                             | Description                                                         |
| ------------- |------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------|
| Example | <a href="">centrality (COMING SOON)</a>                                                                                                              | Centrality algorithms for finding important nodes                   |
| Example | <a href="">community (COMING SOON)</a>                                                                                                               | Community detection for finding clusters                            |
| Example | <a href="https://docs.raphtory.com/en/v0.0.11/api/_autosummary/raphtory.algorithms.html#raphtory.algorithms.global_reciprocity">reciprocity</a>      | Measuring the symmetry of relationships in a graph                  |
| Example | <a href="https://docs.raphtory.com/en/v0.0.11/api/_autosummary/raphtory.algorithms.html#raphtory.algorithms.local_triangle_count">triangle count</a> | Calculates the number of triangles (a cycle of length 3) for a node |

#### 3. Developing an end-to-end application

| Type | Location                                                                                                                                    | Description |
| ------------- |---------------------------------------------------------------------------------------------------------------------------------------------| ------------- |
| Notebook | <a href="https://github.com/Raphtory/Raphtory/blob/master/examples/py/nft/nft_analysis.ipynb">nft_analysis.ipynb</a>                        | Analysing pump and dump cycles of popular NFTs |
| Notebook | <a href="https://github.com/Raphtory/Raphtory/blob/master/examples/py/companies-house/companies_house_example.ipynb">ppe_analysis.ipnyb</a> | Fraud and COVID-19 Relief Schemes |

# Want to run your own analysis?
Learn how to use Raphtory in your analysis and project by following these links.

- **[Latest documentation](https://docs.raphtory.com/)**
- [Using Raphtory in 100 seconds](https://docs.raphtory.com/en/master/Introduction/ingestion.html)
- [Complete list of available algorithms](https://docs.raphtory.com/en/v0.0.11/api/_autosummary/raphtory.algorithms.html)
- [Writing your own algorithm in Raphtory (COMING SOON)]()

# Bounty board

Raphtory is currently offering rewards for contributions, such as new features or algorithms. Contributors will receive swag and prizes! 

To get started, check out our list of desired algorithms at https://www.raphtory.com/algorithm-bounty/ which include some low hanging fruit (🍇) that are easy to implement. 


# Community  
Join the growing community of open-source enthusiasts using Raphtory to power their graph analysis projects!

- Follow [![Slack](https://img.shields.io/twitter/follow/raphtory?label=@raphtory)](https://twitter.com/raphtory) for the latest Raphtory news and development

- Join our [![Slack](https://img.shields.io/badge/community-Slack-red)](https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA) to chat with us and get answers to your questions!


#### Articles and Talks about Raphtory
- **[Raphtory on the Alan Turing Institute Blog](https://www.turing.ac.uk/blog/just-add-time-dizzying-potential-dynamic-graphs)**
- **[Talk on Raphtory at AI UK 2022](https://www.youtube.com/watch?v=7S9Ymnih-YM&list=PLuD_SqLtxSdVEUsCYlb5XjWm9D6WuNKEz&index=9)**
- **[Talk on Raphtory at KGC 2022](https://www.youtube.com/watch?v=37S4bSN5EaU)**
- **[Talk on Raphtory at NetSciX 2022](https://www.youtube.com/watch?v=QxhrONca4FE)**


# Contributors

<a href="https://github.com/raphtory/raphtory/graphs/contributors"><img src="https://contrib.rocks/image?repo=raphtory/raphtory"/></a>

Want to get involved? Please join the Raphtory [Slack](https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA) group and speak with us on how you could pitch in!

# License  

Raphtory is licensed under the terms of the Apache License (check out our LICENSE file).



