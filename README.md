<p align="center">
  <img src="https://raphtory.github.io/images/raphtory_logo.png" alt="Raphtory" width="200" height="200"/>
</p>

# Raphtory 

Open-source platform that brings you distributed real-time temporal graph analytics. Quick and dynamic way to analyse your data with a time element. 

## Table of Contents
- [Getting started](#getting-started)
- [Concept](#concept)
- [Community and Changelog](#community-and-changelog)
- [Contributors](#contributors)
- [License](#license)

## Getting Started

We will use an example Raphtory project and install SBT (Scala Build Tool) to get it up and running.  

1. Clone our [example](https://github.com/Raphtory/Examples) and get the [latest release](https://github.com/Raphtory/Raphtory/releases/latest). 
2. Download the `raphtory.jar` [here](https://github.com/Raphtory/Raphtory/releases/latest/download/raphtory.jar). Move it into the lib directory of the example project.
3. Install SBT by following their [guide](https://www.scala-sbt.org/1.x/docs/Setup.html). The example project uses SBT to compile the source code. 
4. Initiate SBT by changing into the example project directory in your terminal and running the command `sbt`. You are in the SBT interactive shell once you see `>`.
5. Run `compile` to build the project. 
6. Run `run` to analyse the project. The output is the result of the analysis.   

Go to the [documentation](https://raphtory.github.io/documentation/install) for information on writing your own analysis and more.

Raphtory is built with Scala. We recommend using IntelliJ IDE for your code. They have a community version which is free. Follow their [guide](https://www.jetbrains.com/idea/download/#section=windows) for installation.

## Concept  

Static graphs, which requires data to be manually reloaded for an update, have been the focus for data analytics so far in the community. While static graphs are useful, they could be inefficient. **Our tool aims to address these issues by helping you create dynamic graphs with the added element of time.** Dynamic graphs allow for a more efficient process with its ability to automatically make changes in real time by intaking streamed or stored data.  Our tool also gives you the option to dive deep in your analysis and explore the changes in structural (vertex and edge properties) and temporal (time of when components of your graph were created, updated, and deleted) scopes of your graphs. This opens up many possibilities including tracking cryptocurrency fraud, covid transmission patterns, or monitoring activities on social media.  

Check out these links for more information:

- [Detailed overview](https://raphtory.github.io/) on the research
- [Introduction to temporal graphs](https://chorograph.com/demo) and see some of the exciting software built on top of Raphtory
- Raphtory [article](https://www.turing.ac.uk/blog/just-add-time-dizzying-potential-dynamic-graphs) on the Alan Turing Institute site

## Community and Changelog  

- Follow the latest development on our [blog](https://raphtory.github.io/blog/)
- Follow us on [Twitter](https://twitter.com/raphtory)
- Join our [Slack](https://join.slack.com/t/raphtory/shared_invite/zt-jd5mce91-vDxEiFBILC_G2ilZPdvDaA) group (we are always happy to answer any questions and chat about the project!) Feel free to join the #raphtory-development and #askaway channel to discuss current issues, ask your questions in #general or ping Ben Steer (miratepuffin) who can give you a hand.

## Contributors

Want to get involved with the development? Please join our [Slack](https://join.slack.com/t/raphtory/shared_invite/zt-jd5mce91-vDxEiFBILC_G2ilZPdvDaA) and speak with us on how you could pitch in!

## License  

Raphtory is licensed under the terms of the Apache License (check out our LICENSE file).



