<br>
<p align="center">
  <img src="https://user-images.githubusercontent.com/6665739/130641943-fa7fcdb8-a0e7-4aa4-863f-3df61b5de775.png" alt="Raphtory" height="100"/>
</p>
<p align="center">
</p>


<p align="center">
<a href="https://github.com/Raphtory/Raphtory/actions/workflows/test_and_build.yml?query=event%3Aschedule++">
<img alt="Test and Build" src="https://github.com/Raphtory/Raphtory/actions/workflows/test.yml/badge.svg?event=schedule" />
</a>
<a href="https://github.com/Raphtory/Raphtory/releases">
<img alt="Latest Release" src="https://img.shields.io/github/v/release/Raphtory/Raphtory?color=brightgreen&include_prereleases" />
</a>
<a href="https://github.com/Raphtory/Raphtory/issues">
<img alt="Issues" src="https://img.shields.io/github/issues/Raphtory/Raphtory?color=brightgreen" />
</a>
<a href="https://mybinder.org/v2/gh/Raphtory/Raphtory/master?labpath=examples%2Fbinder_python%2Findex.ipynb">
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
<a href="https://docs.raphtory.com/en/master/Examples/lotr.html">🧙🏻‍ Tutorial</a> 
&nbsp
<a href="https://github.com/Raphtory/Raphtory/issues">🐛 Report a Bug</a> 
&nbsp
<a href="https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA"><img src="https://user-images.githubusercontent.com/6665739/154071628-a55fb5f9-6994-4dcf-be03-401afc7d9ee0.png" height="20" align="center"/> Join Slack</a> 
</p>

<br>


Raphtory is a unified analytics engine for large-scale graph analysis, allowing you to run complex graph queries no matter where your data lives and what format it is in. Besides performance and scalability, what makes Raphtory cool is its ability to represent and explore the history of a complex system, from simply “time travelling” through data, to executing richer analysis like **taint tracking**, **temporal reachability**, or **mining temporal motifs**.

**Raphtory is easy to use:** One-line pip installation and smooth integration with Pandas for input and output.

**Raphtory is expressive:** Designed to represent all types of graph queries and temporal graphs in mind, with a thoughtfully developed API for exploring your data across its history.

**Raphtory is scalable:** Built on top of Apache Arrow’s storage model and gRPC for client communication, Raphtory can be run on a laptop or scaled up to a cluster for results on terabyte scale graphs.

#### Articles and Talks about Raphtory
- **[Raphtory on the Alan Turing Institute Blog](https://www.turing.ac.uk/blog/just-add-time-dizzying-potential-dynamic-graphs)**
- **[Talk on Raphtory at AI UK 2022](https://www.youtube.com/watch?v=7S9Ymnih-YM&list=PLuD_SqLtxSdVEUsCYlb5XjWm9D6WuNKEz&index=9)**
- **[Talk on Raphtory at KGC 2022](https://www.youtube.com/watch?v=37S4bSN5EaU)**
- **[Talk on Raphtory at NetSciX 2022](https://www.youtube.com/watch?v=QxhrONca4FE)**

# Running a basic example

```python
# Import Raphtory
import PyRaphtory

# Create a new local or distributed context
ctx = PyRaphtory.local()
graph = ctx.new_graph()

# Add some data to your graph
graph.add_vertex(1, 1)
graph.add_vertex(2, 2)
graph.add_vertex(3, 3)
graph.add_edge(4, 1, 2)
graph.add_edge(4, 1, 3)

# Collect some simple vertex metrics
# Ran across a range of the data with incremental windowing
df = graph
      .range(1,4,1)
      .window(1)
      .select(lambda vertex: Row(vertex.name(), vertex.out_degree(), vertex.in_degree()))
      .to_df(["name", "out_degree", "in_degree"])

# Preview DataFrame
df

|    |   timestamp |   window |   name |   out_degree |   in_degree |
|---:|------------:|---------:|-------:|-------------:|------------:|
|  0 |           1 |        1 |      1 |            0 |           0 |
|  1 |           2 |        1 |      2 |            0 |           0 |
|  2 |           3 |        1 |      3 |            0 |           0 |
|  3 |           4 |        1 |      1 |            2 |           0 |
|  4 |           4 |        1 |      2 |            0 |           1 |
|  5 |           4 |        1 |      3 |            0 |           1 |
```

You can try out Raphtory for yourself in a Jupyter Notebook. Please click here [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/Raphtory/Raphtory/v0.2.0a9?labpath=examples%2Fbinder_python%2Findex.ipynb) to launch the notebook.

# Installing Raphtory 
Raphtory is available for both Python and Scala / Java, with support for Rust planned in the 0.3.0 release. 

We recommend installing the PyRaphtory client for Python, which contains all the functions to create a graph, run algorithms and analyse results. We also support Conda, but pip is the preferred option.

Make sure you have **Python v3.9 or above**. It is also recommended that you install Raphtory through conda, virtualenv or pyenv as a best practice. 

```bash
pip install pyraphtory_jvm pyraphtory
```

# Want to do something more complex?
If you would like to do something more complex, follow these links:

- **[Documentation](https://docs.raphtory.com/)**
- [Complete list of available algorithms in Raphtory](https://docs.raphtory.com/en/master/_autodoc/com/raphtory/algorithms/generic/index.html)
- [Writing your own algorithm in Raphtory](https://docs.raphtory.com/en/master/Analysis/LOTR_six_degrees.html)

# Bounty board

We are currently running a bounty board for anyone that wishes to contribute to Raphtory, whether it be cool features or yet to be implemented algorithms.

All contributors receive swag and small prizes!

To get you started, we currently have a list of algorithms that we would like to build into Raphtory. With a number of 🍇 low hanging fruit ones. Make sure to check it out at https://www.raphtory.com/algorithm-bounty/ 


# Community  

- [![Slack](https://img.shields.io/twitter/follow/raphtory?label=Follow)](https://twitter.com/raphtory) the latest developments on the official Raphtory Twitter

- [![Slack](https://img.shields.io/badge/Join%20Our%20Community-Slack-red)](https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA) - we are always happy to answer any questions and chat about the project! Feel free to join the #raphtory-development and #askaway channel to discuss current issues or ask any questions.


# Contributors

<a href="https://github.com/raphtory/raphtory/graphs/contributors"><img src="https://contrib.rocks/image?repo=raphtory/raphtory"/></a>

Want to get involved? Please join the Raphtory [Slack](https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA) group and speak with us on how you could pitch in!

# License  

Raphtory is licensed under the terms of the Apache License (check out our LICENSE file).



