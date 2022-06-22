<p align="center">
  <img src="https://user-images.githubusercontent.com/6665739/130641943-fa7fcdb8-a0e7-4aa4-863f-3df61b5de775.png" alt="Raphtory" height="250"/>
</p>

[![test and build](https://github.com/Raphtory/Raphtory/actions/workflows/test_and_build.yml/badge.svg?branch=master&event=push)](https://github.com/Raphtory/Raphtory/actions/workflows/test_and_build.yml?query=branch%3Amaster+event%3Apush++)
[![test and build](https://github.com/Raphtory/Raphtory/actions/workflows/test_and_build.yml/badge.svg?event=schedule)](https://github.com/Raphtory/Raphtory/actions/workflows/test_and_build.yml?query=event%3Aschedule++)
[![Latest Tag](https://img.shields.io/github/v/tag/Raphtory/Raphtory?include_prereleases&sort=semver&color=brightgreen)](https://github.com/Raphtory/Raphtory/tags)
[![Latest Release](https://img.shields.io/github/v/release/Raphtory/Raphtory?color=brightgreen&include_prereleases&sort=semver)](https://github.com/Raphtory/Raphtory/releases)
[![Conventional Commits](https://img.shields.io/badge/Conventional%20Commits-1.0.0-yellow.svg)](https://conventionalcommits.org)

Raphtory is an open-source platform for distributed real-time temporal graph analytics, allowing you to load and process large dynamic datasets across time. If you would like a brief summary of what its used for before fully diving into the getting started guide please check out this [article](https://www.turing.ac.uk/blog/just-add-time-dizzying-potential-dynamic-graphs) from the Alan Turing Institute! For more in-depth info you can watch our most recent talk on Raphtory at [AIUK 2022](https://www.youtube.com/watch?v=7S9Ymnih-YM&list=PLuD_SqLtxSdVEUsCYlb5XjWm9D6WuNKEz&index=9), [KGC 2022](https://www.youtube.com/watch?v=37S4bSN5EaU) and [NetSciX](https://www.youtube.com/watch?v=QxhrONca4FE).

<p align="center">
<img src="https://user-images.githubusercontent.com/6665739/154071628-a55fb5f9-6994-4dcf-be03-401afc7d9ee0.png"/> If you like the sound of what we are working on, come join the <a href="https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA">Slack</a>! <img src="https://user-images.githubusercontent.com/6665739/154071628-a55fb5f9-6994-4dcf-be03-401afc7d9ee0.png"/>
</p>

## 0.1.0 - Raphtory Post Alpha Release
With the release of 0.1.0, we have completely rebuilt Raphtory from the ground up. The full changelog can be dug into [here](https://github.com/Raphtory/Raphtory/releases), but as a sneak peek this includes:

* A brand new Analytical API with expressive windowing and history exploration, composable algorithms/chaining, global aggregators/histograms, filters/sampling, multilayer graph projections, clearer output formats and much more.
* A communication layer which allows for fine-grained management of how Raphtory components send messages. This by default is split between [Apache Pulsar](https://pulsar.apache.org) for updates and vertex messages and [Akka](https://akka.io) for control messages. This fixes a number of issues faced in prior versions, notably around message back pressure and cluster management.
* An initial Python client for Raphtory which will be brought fully in line with the Scala version over the coming months.   

This has replaced the now deprecated Raphtory Alpha which will remain available [here](https://github.com/Raphtory/Raphtory/tree/raphtory-akka).


# Getting Started
The best way to get started with Raphtory is to visit our [ReadTheDocs](https://raphtory.readthedocs.io/en/master/) site where we have tutorials on how to use Raphtory for graph building, analysis, and more. 

**Note:** Raphtory is built with Scala. We recommend using IntelliJ IDE for your code. They have a community version which is free. Follow their [guide](https://www.jetbrains.com/idea/download/#section=windows) for installation.

### Good entry points for the very beginning are

- [Installation](https://raphtory.readthedocs.io/en/development/Install/installdependencies.html) - Installation of Raphtory and running your first job locally.
- [Building a graph from your data](https://raphtory.readthedocs.io/en/development/Ingestion/sprouter.html) - How to ingest raw data into Raphtory and the basics of modelling your data as a Temporal Graph.
- [Six Degrees of Gandalf](https://raphtory.readthedocs.io/en/development/Analysis/LOTR_six_degrees.html) - A Lord of the Rings themed intro into the world of Graph Algorithms.
- [Running queries across time](https://raphtory.readthedocs.io/en/development/Analysis/queries.html) - Exploring how to run algorithms throughout the history of your data. 
- [Analysis In Raphtory](https://raphtory.readthedocs.io/en/development/Analysis/analysis-explained.html) - A deeper dive into the underlying analysis model of Raphtory. 

### Once you are feeling more comfortable with Raphtory you can checkout out
- [ScalaDocs](https://raphtory.readthedocs.io/en/development/Scaladoc/index.html) and [Algorithm API](https://raphtory.readthedocs.io/en/development/_autodoc/com/raphtory/algorithms/generic/index.html) - We provide a fully documented API for Raphtory, explaining all user-facing classes and functions. This sits alongside explanations for the included algorithmic library (both temporal and static). These algorithms can be used to analyse your datasets once ingested or as a basis to implement your own custom algorithms.
- [Raphtory Streaming and Distributed Deployment](https://raphtory.readthedocs.io/en/development/Deployment/pulsarlocal.html) - Raphtory can be deployed as a single node or a distributed cluster. For the latter, we provide runners to establish the cluster on bare metal or on top of Kubernetes. 
- [Using the Python client](https://raphtory.readthedocs.io/en/development/PythonClient/setup.html) - We are developing a full python client alongside the main scala implementation so that you can attach Raphtory to your favorite data science tools.


# Community  

- Follow the latest developments on the official Raphtory [Twitter](https://twitter.com/raphtory)
- Join the [Slack](https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA) - we are always happy to answer any questions and chat about the project! Feel free to join the #raphtory-development and #askaway channel to discuss current issues or ask any questions.

# Contributors

<a href="https://github.com/raphtory/raphtory/graphs/contributors"><img src="https://contrib.rocks/image?repo=raphtory/raphtory"/></a>

Want to get involved? Please join the Raphtory [Slack](https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA) group and speak with us on how you could pitch in!

# License  

Raphtory is licensed under the terms of the Apache License (check out our LICENSE file).



