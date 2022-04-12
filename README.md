<p align="center">
  <img src="https://user-images.githubusercontent.com/6665739/130641943-fa7fcdb8-a0e7-4aa4-863f-3df61b5de775.png" alt="Raphtory" height="250"/>
</p>

[![test and build](https://github.com/Raphtory/Raphtory/actions/workflows/test_and_build.yml/badge.svg?branch=master&event=push)](https://github.com/Raphtory/Raphtory/actions/workflows/test_and_build.yml?query=branch%3Amaster+event%3Apush++)
[![test and build](https://github.com/Raphtory/Raphtory/actions/workflows/test_and_build.yml/badge.svg?event=schedule)](https://github.com/Raphtory/Raphtory/actions/workflows/test_and_build.yml?query=event%3Aschedule++)
[![Latest Tag](https://img.shields.io/github/v/tag/Raphtory/Raphtory?include_prereleases&sort=semver&color=brightgreen)](https://github.com/Raphtory/Raphtory/tags)
[![Latest Release](https://img.shields.io/github/v/release/Raphtory/Raphtory?color=brightgreen&include_prereleases&sort=semver)](https://github.com/Raphtory/Raphtory/releases)
[![Conventional Commits](https://img.shields.io/badge/Conventional%20Commits-1.0.0-yellow.svg)](https://conventionalcommits.org)

Raphtory is an open-source platform for distributed real-time temporal graph analytics, allowing you to load and process large dynamic datasets across time. If you would like a brief summary of what its used for before fully diving into the getting started guide please check out this [article](https://www.turing.ac.uk/blog/just-add-time-dizzying-potential-dynamic-graphs) from the Alan Turing Institute! For more in-depth info you can watch our most recent talk on Raphtory at [NetSciX](https://www.youtube.com/watch?v=QxhrONca4FE).

<p align="center">
<img src="https://user-images.githubusercontent.com/6665739/154071628-a55fb5f9-6994-4dcf-be03-401afc7d9ee0.png"/> If you like the sound of what we are working on, come join the <a href="https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA">Slack</a>! <img src="https://user-images.githubusercontent.com/6665739/154071628-a55fb5f9-6994-4dcf-be03-401afc7d9ee0.png"/>
</p>

## 0.5.0 - Raphtory on Pulsar
With the release of 0.5.0 we have completely rebuilt Raphtory to run on top of [Apache Pulsar](https://pulsar.apache.org). This has fixed a number of issues faced in prior versions, notably around message back pressure, and introduces many exciting features including integration with Jupyter. This has replaced the now deprecated Akka implementation which will remain available [here](https://github.com/Raphtory/Raphtory/tree/raphtory-akka).


# Getting Started
The best way to get started with Raphtory is to visit our [ReadTheDocs](https://raphtory.readthedocs.io/en/master/) site where we have tutorials on how to use Raphtory for graph building, analysis, and more. 

**Note:** Raphtory is built with Scala. We recommend using IntelliJ IDE for your code. They have a community version which is free. Follow their [guide](https://www.jetbrains.com/idea/download/#section=windows) for installation.

### Good entry points for the very beginning are

- [Installation](https://raphtory.readthedocs.io/en/master/Install/installdependencies.html) - Installation of Pulsar & Raphtory and running your first job locally.
- [Building a graph from your data](https://raphtory.readthedocs.io/en/master/Ingestion/sprouter.html) - How to ingest raw data into Raphtory and the basics of modelling your data as a Temporal Graph.
- [Six Degrees of Gandalf](https://raphtory.github.io/documentation/analysis-qs) - A Lord of the Rings themed intro into the world of Graph Algorithms.
- [Running Queries](https://raphtory.readthedocs.io/en/master/Analysis/queries.html) - Exploring how to run algorithms throughout the history of your data. 
- [Analysis In Raphtory](https://raphtory.readthedocs.io/en/development/Analysis/analysis-explained.html) - A deeper dive into the underlying analysis model of Raphtory. 

### Once you are feeling more comfortable with Raphtory you can checkout out
- [Raphtory API](https://raphtory.readthedocs.io/en/development/_autodoc/com/raphtory/algorithms/index.html) - We provide a fully documented API for Raphtory with explanations of all included Algorithms (both temporal and static). These algorithms can be used to analyse your datasets once ingested or as a basis to implement your own custom algorithms.
- [Getting your data into Jupyter](https://raphtory.readthedocs.io/en/development/PythonClient/tutorial.html#) - We are developing a full python client alongside the main scala implementation so that you can attach Raphtory to your favorite data science tools.
- [Kubernetes Cluster Deployment](https://raphtory.readthedocs.io/en/development/Deployment/kubernetes.html) - Raphtory can be deployed as a single node or a distributed cluster. For the latter we provide runners to establish the cluster on top of Kubernetes. 

# Community  

- Follow the latest developments on the official Raphtory [Twitter](https://twitter.com/raphtory)
- Join the [Slack](https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA) - we are always happy to answer any questions and chat about the project! Feel free to join the #raphtory-development and #askaway channel to discuss current issues or ask any questions.

# Contributors

<a href="https://github.com/raphtory/raphtory/graphs/contributors"><img src="https://contrib.rocks/image?repo=raphtory/raphtory"/></a>

Want to get involved? Please join the Raphtory [Slack](https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA) group and speak with us on how you could pitch in!

# License  

Raphtory is licensed under the terms of the Apache License (check out our LICENSE file).



