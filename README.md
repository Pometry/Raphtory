<p align="center">
  <img src="https://user-images.githubusercontent.com/6665739/130641943-fa7fcdb8-a0e7-4aa4-863f-3df61b5de775.png" alt="Raphtory" height="250"/>
</p>

Open-source platform for distributed real-time temporal graph analytics. Load and process large dynamic graphs across time.

[![test and build](https://github.com/Raphtory/Raphtory/actions/workflows/test_and_build.yml/badge.svg?branch=master&event=push)](https://github.com/Raphtory/Raphtory/actions/workflows/test_and_build.yml?query=branch%3Amaster+event%3Apush++)
[![test and build](https://github.com/Raphtory/Raphtory/actions/workflows/test_and_build.yml/badge.svg?event=schedule)](https://github.com/Raphtory/Raphtory/actions/workflows/test_and_build.yml?query=event%3Aschedule++)
[![Latest Tag](https://img.shields.io/github/v/tag/Raphtory/Raphtory?include_prereleases&sort=semver&color=brightgreen)](https://github.com/Raphtory/Raphtory/tags)
[![Latest Release](https://img.shields.io/github/v/release/Raphtory/Raphtory?color=brightgreen&include_prereleases&sort=semver)](https://github.com/Raphtory/Raphtory/releases)

## Table of Contents
- [Getting started](#getting-started)
- [Concept](#concept)
- [Community and Changelog](#community-and-changelog)
- [Contributors](#contributors)
- [License](#license)

## Getting Started

Let's use an example Raphtory project and install SBT (Scala Build Tool) to get it up and running.  

1. Clone the [example](https://github.com/Raphtory/Examples) and get the [latest release](https://github.com/Raphtory/Raphtory/releases/latest). 
2. Download the `raphtory.jar` [here](https://github.com/Raphtory/Raphtory/releases/latest/download/raphtory.jar). Move it into the lib directory of the example project.
3. Install SBT by following their [guide](https://www.scala-sbt.org/1.x/docs/Setup.html). The example project uses SBT to compile the source code. 
4. Initiate SBT by changing into the example project directory in the terminal and running the command `sbt`. The SBT interactive shell starts once it shows `>`.
5. Run `compile` to build the project. 
6. Run `run` to analyse the project. The output is the result of the analysis.   

Go to the [documentation](https://raphtory.github.io/documentation/install) for [tutorials](https://raphtory.github.io/documentation/sprouter) on how to use Raphtory for graph building, analysis, and more.  

Raphtory is built with Scala. We recommend using IntelliJ IDE for your code. They have a community version which is free. Follow their [guide](https://www.jetbrains.com/idea/download/#section=windows) for installation.

## Concept  

Static graphs, which require data to be manually reloaded for updates, have been the focus for data analytics in the community. While static graphs are useful, they could be inefficient. **Raphtory aims to address these issues by creating dynamic graphs with the added element of time.** Dynamic graphs allow for a more efficient process with its ability to automatically make changes in real time by intaking streamed or stored data.  Raphtory also gives the option to dive deep in analysis and explore the changes in structural (vertex and edge properties) and temporal (time of when components of your graph are created, updated, and deleted) scopes of the graphs. This opens up many possibilities including tracking cryptocurrency fraud, covid transmission patterns, or monitoring activities on social media.  

Check out these links for more information:

- [Detailed overview](https://raphtory.github.io/) on the research
- Raphtory [article](https://www.turing.ac.uk/blog/just-add-time-dizzying-potential-dynamic-graphs) on the Alan Turing Institute site

## Community and Changelog  

- Follow the latest development on the official [blog](https://raphtory.github.io/blog/)
- Follow the Raphtory [Twitter](https://twitter.com/raphtory)
- Join the [Slack](https://join.slack.com/t/raphtory/shared_invite/zt-jd5mce91-vDxEiFBILC_G2ilZPdvDaA) group (we are always happy to answer any questions and chat about the project!) Feel free to join the #raphtory-development and #askaway channel to discuss current issues, ask your questions in #general or ping Ben Steer (miratepuffin) who can give you a hand.

## Contributors

<a href="https://github.com/raphtory/raphtory/graphs/contributors"><img src="https://contrib.rocks/image?repo=raphtory/raphtory"/></a>

Want to get involved? Please join the Raphtory [Slack](https://join.slack.com/t/raphtory/shared_invite/zt-jd5mce91-vDxEiFBILC_G2ilZPdvDaA) group and speak with us on how you could pitch in!

## License  

Raphtory is licensed under the terms of the Apache License (check out our LICENSE file).



