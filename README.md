<p align="center">
  <img src="https://user-images.githubusercontent.com/6665739/130641943-fa7fcdb8-a0e7-4aa4-863f-3df61b5de775.png" alt="Raphtory" height="100"/>
</p>
<br>

[![test and build](https://github.com/Raphtory/Raphtory/actions/workflows/test_and_build.yml/badge.svg?branch=master&event=push)](https://github.com/Raphtory/Raphtory/actions/workflows/test_and_build.yml?query=branch%3Amaster+event%3Apush++)
[![test and build](https://github.com/Raphtory/Raphtory/actions/workflows/test_and_build.yml/badge.svg?event=schedule)](https://github.com/Raphtory/Raphtory/actions/workflows/test_and_build.yml?query=event%3Aschedule++)
[![Latest Release](https://img.shields.io/github/v/release/Raphtory/Raphtory?color=brightgreen&include_prereleases&sort=semver)](https://github.com/Raphtory/Raphtory/releases)
[![Conventional Commits](https://img.shields.io/badge/Conventional%20Commits-1.0.0-yellow.svg)](https://conventionalcommits.org)
<a href="https://github.com/Raphtory/Raphtory/issues">
<img alt="Issues" src="https://img.shields.io/github/issues/Raphtory/Raphtory?color=brightgreen" />
</a>
</br>

Raphtory is an open-source platform for distributed real-time temporal graph analytics, allowing you to load and process large dynamic datasets across time. 

Features of Raphtory include:
1. No data movement required - easy to pull in data from **anywhere**
2. Easy to **scale**
3. Easy to **distribute** and **elastic**
4. Familiar functions to **Pandas** and **NetworkX**

### Useful Links
- **[Website 🌍](https://www.raphtory.com/)**
- **[Documentation 📒](https://docs.raphtory.com/en/development/)**
- **[Source Code 🖥](https://github.com/Raphtory/Raphtory)**
- **[Tutorial 🧙🏻‍](https://docs.raphtory.com/en/development/Examples/lotr.html)**
- **[Report a Bug 🐛](https://github.com/Raphtory/Raphtory/issues)**

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

Install PyRaphtory, which contains all the functions to create a graph, run algorithms and analyse results.
```bash
pip install requests pandas pemja cloudpickle parsy
pip install -i https://test.pypi.org/simple/ pyraphtory_jvm==0.2.0a7
pip install -i https://test.pypi.org/simple/ pyraphtory==0.2.0a7
```

# Basic Example

Initialise Raphtory and create a new graph.
```python
ctx = PyRaphtory.local()
graph = ctx.new_graph()
```

Add nodes and edges to your graph from any file (here we used a CSV file).
```python
graph.add_vertex(1, 1)
graph.add_vertex(1, 2)
graph.add_vertex(1, 3)
graph.add_edge(2, 1, 2)
graph.add_edge(2, 1, 3)
```
Collect simple metrics from your graph.
```python
df = graph
    .select(lambda vertex: Row(vertex.name(), vertex.out_degree(), vertex.in_degree()))
    .to_df(["name", "out_degree", "in_degree"])
```
Check out the output.
```python
df
```
|    |   timestamp | window   |   name |   out_degree |   in_degree |
|---:|------------:|:---------|-------:|-------------:|------------:|
|  0 |           2 |          |      1 |            2 |           0 |
|  1 |           2 |          |      2 |            0 |           1 |
|  2 |           2 |          |      3 |            0 |           1 |

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



