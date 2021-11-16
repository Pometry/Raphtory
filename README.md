<p align="center">
  <img src="https://user-images.githubusercontent.com/6665739/130641943-fa7fcdb8-a0e7-4aa4-863f-3df61b5de775.png" alt="Raphtory" height="250"/>
</p>

[![test and build](https://github.com/Raphtory/Raphtory/actions/workflows/test_and_build.yml/badge.svg?branch=master&event=push)](https://github.com/Raphtory/Raphtory/actions/workflows/test_and_build.yml?query=branch%3Amaster+event%3Apush++)
[![test and build](https://github.com/Raphtory/Raphtory/actions/workflows/test_and_build.yml/badge.svg?event=schedule)](https://github.com/Raphtory/Raphtory/actions/workflows/test_and_build.yml?query=event%3Aschedule++)
[![Latest Tag](https://img.shields.io/github/v/tag/Raphtory/Raphtory?include_prereleases&sort=semver&color=brightgreen)](https://github.com/Raphtory/Raphtory/tags)
[![Latest Release](https://img.shields.io/github/v/release/Raphtory/Raphtory?color=brightgreen&include_prereleases&sort=semver)](https://github.com/Raphtory/Raphtory/releases)

Raphtory is an open-source platform for distributed real-time temporal graph analytics, allowing you to load and process large dynamic datsets across time. If you would like a brief summary of what its used for before fully diving into the getting start guide please check out this [article](https://www.turing.ac.uk/blog/just-add-time-dizzying-potential-dynamic-graphs) from the Alan Turing Institute first!

## Table of Contents
- [Getting started](#getting-started)
- [Community and Changelog](#community-and-changelog)
- [Contributors](#contributors)
- [License](#license)

## Getting Started
The best way to get started with Raphtory is to vist our [website](https://raphtory.github.io/) where we have tutorials on how to use Raphtory for graph building, analysis, and more. Good entry points for this are:

- [Installation](https://raphtory.github.io/documentation/install)
- [Building a graph from your data](https://raphtory.github.io/documentation/sprouter)
- [Running algorithms and writing your own](https://raphtory.github.io/documentation/analysis-qs)
- [Distributed Deployment](https://raphtory.github.io/documentation/analysis-qs)

We also have a [page](https://raphtory.github.io/algorithms/) for algorithms implemented in Raphtory (both temporal and static). These can be used to analyse your own datasets once ingested or as a basis to implement your own custom algorithms.


### Too Long Didn't Read - Let me run it now!
If you want to see how Raphtory runs without reading a mountain of documentation you can quickly get set up with an example Raphtory project via these steps:

1. Clone the [example](https://github.com/Raphtory/Examples) repo and pick one of the examples inside that takes your fancy. 
2. Download the [latest stable release](https://github.com/Raphtory/Raphtory/releases/latest) of the `raphtory.jar` and move it into the lib directory of your chosen example project. 
   * **Here be dragons:** You can also pick the nightly build, but there may be some quirks yet to be ironed out. If you find any please report the issue on git or on slack.
3. Install SBT by following their [guide](https://www.scala-sbt.org/1.x/docs/Setup.html). The example project uses SBT to compile the source code. 
4. Initiate SBT by changing into the example project directory in the terminal and running the command `sbt`. You will know when the SBT interactive shell has started once it shows `>`.
5. Execute `compile` to build the project. 
6. Execute `run` to start the project. You will then see Raphtory build the graph and execute an algorithm relevant to your chosen dataset.
7. The rest is then up to you - feel free to explore the data, submit different algorithms and ask any questsions you have on the Raphtory [Slack](https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA)!   

**Note:** Raphtory is built with Scala. We recommend using IntelliJ IDE for your code. They have a community version which is free. Follow their [guide](https://www.jetbrains.com/idea/download/#section=windows) for installation.


## Community and Changelog  

- Follow the latest development on the official [blog](https://raphtory.github.io/blog/)
- Follow the Raphtory [Twitter](https://twitter.com/raphtory)
- Join the [Slack](https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA) group (we are always happy to answer any questions and chat about the project!) Feel free to join the #raphtory-development and #askaway channel to discuss current issues, ask your questions or raise issues.

## Contributors

<a href="https://github.com/raphtory/raphtory/graphs/contributors"><img src="https://contrib.rocks/image?repo=raphtory/raphtory"/></a>

Want to get involved? Please join the Raphtory [Slack](https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA) group and speak with us on how you could pitch in!

## License  

Raphtory is licensed under the terms of the Apache License (check out our LICENSE file).



