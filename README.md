<br>
<p align="center">
  <img src="https://user-images.githubusercontent.com/6665739/130641943-fa7fcdb8-a0e7-4aa4-863f-3df61b5de775.png" alt="Raphtory" height="100"/>
</p>

<br>
<p align="center">
<a href="https://github.com/Raphtory/Raphtory/actions/workflows/test_and_build.yml?query=event%3Aschedule++">
<img alt="Test and Build" src="https://github.com/Raphtory/Raphtory/actions/workflows/test_and_build.yml/badge.svg?event=schedule" />
</a>
<a href="https://github.com/Raphtory/Raphtory/releases">
<img alt="Latest Release" src="https://img.shields.io/github/v/release/Raphtory/Raphtory?color=brightgreen&include_prereleases&sort=semver" />
</a>
<a href="https://github.com/Raphtory/Raphtory/issues">
<img alt="Issues" src="https://img.shields.io/github/issues/Raphtory/Raphtory?color=brightgreen" />
</a>
</p>

<p align="center">
<a href="https://www.raphtory.com/"><b>Website 🌍</b></a> <a href="https://docs.raphtory.com/en/development/"><b>Documentation 📒</b></a> <a href="https://github.com/Raphtory/Raphtory"><b>Source Code 🖥</b></a> <a href="https://docs.raphtory.com/en/development/Examples/lotr.html"><b>Tutorial 🧙🏻</b></a> <a href="https://github.com/Raphtory/Raphtory/issues"><b>Report a Bug 🐛</b></a>
</p>

Raphtory is a unified analytics engine for **distributed graph analytics**, which allows you to run complex graph queries **no matter where your data lives** and **what format** it is in. What makes Raphtory unique is its combination of a **graph model with time-series analysis**, providing a powerful model for extracting new **insights**. Raphtory supports both **traditional graph analysis** (e.g. shortest path and community detection), as well as **trend and anomaly detection** via time-series. Things get interesting when the two are combined, allowing you to **"time travel"** through your data, look for **time-respecting paths**, identify **causal relationships**, model the evolution of **communities over time** or extract **temporal patterns in the interactions** between entities in the network.

Features of Raphtory include:
1. **No data movement** required, read your data as a graph no matter storage and format
2. Easy to install and run, with functions familiar to **NetworkX** and easy integration with **Pandas**
3. **Performant and scalable**, with Apache Arrow storage model and gRPC client communication

### Articles and Talks about Raphtory
- **[Raphtory on the Alan Turing Institute Blog](https://www.turing.ac.uk/blog/just-add-time-dizzying-potential-dynamic-graphs)**
- **[Talk on Raphtory at AI UK 2022](https://www.youtube.com/watch?v=7S9Ymnih-YM&list=PLuD_SqLtxSdVEUsCYlb5XjWm9D6WuNKEz&index=9)**
- **[Talk on Raphtory at KGC 2022](https://www.youtube.com/watch?v=37S4bSN5EaU)**
- **[Talk on Raphtory at NetSciX 2022](https://www.youtube.com/watch?v=QxhrONca4FE)**

<p align="center">
<img src="https://user-images.githubusercontent.com/6665739/154071628-a55fb5f9-6994-4dcf-be03-401afc7d9ee0.png"/> If you like the sound of what we are working on, come join our <a href="https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA">Slack Community</a>! <img src="https://user-images.githubusercontent.com/6665739/154071628-a55fb5f9-6994-4dcf-be03-401afc7d9ee0.png"/>
</p>

# Prerequisites
- Python 3.9.15
  - Conda, virtualenv or system installed Python
- Mac and Linux

# Install

Install PyRaphtory, which contains all the functions to create a graph, run algorithms and analyse results. We also support Conda, but pip is the preferred option.

*Make sure Python version > 3.7 is installed.*

```bash
pip install requests pandas pemja cloudpickle parsy
pip install -i https://test.pypi.org/simple/ pyraphtory_jvm==0.2.0a7
pip install -i https://test.pypi.org/simple/ pyraphtory==0.2.0a7
```

# Basic Example

Initialise Raphtory and create a new graph.
```python
import PyRaphtory

ctx = PyRaphtory.local()
graph = ctx.new_graph()
```

Add nodes and edges to your graph from any file (here we used a CSV file).
```python
graph.add_vertex(1, 1)
graph.add_vertex(2, 2)
graph.add_vertex(3, 3)
graph.add_edge(4, 1, 2)
graph.add_edge(4, 1, 3)
```
Collect simple metrics from your graph.
```python
df = graph
      .range(1,4,1)
      .window(1)
      .select(lambda vertex: Row(vertex.name(), vertex.out_degree(), vertex.in_degree()))
      .to_df(["name", "out_degree", "in_degree"])
```
Check out the output.
```python
df
```
|    |   timestamp |   window |   name |   out_degree |   in_degree |
|---:|------------:|---------:|-------:|-------------:|------------:|
|  0 |           1 |        1 |      1 |            0 |           0 |
|  1 |           2 |        1 |      2 |            0 |           0 |
|  2 |           3 |        1 |      3 |            0 |           0 |
|  3 |           4 |        1 |      1 |            2 |           0 |
|  4 |           4 |        1 |      2 |            0 |           1 |
|  5 |           4 |        1 |      3 |            0 |           1 |

# Want to do something more complex?
If you would like to do something more complex, follow these links:

- **[Documentation](https://docs.raphtory.com/en/development/)**
- [Building your own Scala source code into PyRaphtory](https://docs.raphtory.com/en/development/PythonDocs/setup.html#id2)
- [Complete list of available algorithms in Raphtory](https://docs.raphtory.com/en/development/_autodoc/com/raphtory/algorithms/generic/index.html)
- [Writing your own algorithm in Raphtory](https://docs.raphtory.com/en/development/Analysis/LOTR_six_degrees.html)

# Raphtory Latest Release
### 0.2.0a7 
With the release of 0.2.0, we have re-designed and updated many of the core components. The full changelog can be dug into [here](https://github.com/Raphtory/Raphtory/releases).

The prior version of Raphtory, raphtory-akka, has now been deprecated. This will remain available with no support [here](https://github.com/Raphtory/Raphtory/tree/raphtory-akka).

# Community  

- [![Slack](https://img.shields.io/twitter/follow/raphtory?label=Follow)](https://twitter.com/raphtory) the latest developments on the official Raphtory Twitter

- [![Slack](https://img.shields.io/badge/Join%20Our%20Community-Slack-red)](https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA) - we are always happy to answer any questions and chat about the project! Feel free to join the #raphtory-development and #askaway channel to discuss current issues or ask any questions.


# Contributors

<a href="https://github.com/raphtory/raphtory/graphs/contributors"><img src="https://contrib.rocks/image?repo=raphtory/raphtory"/></a>

Want to get involved? Please join the Raphtory [Slack](https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA) group and speak with us on how you could pitch in!

# License  

Raphtory is licensed under the terms of the Apache License (check out our LICENSE file).



