# Raphtory Introduction
Raphtory is an ongoing project to maintain and analyse temporal graphs built from event streams within a distributed environment. Raphtory takes any source of data (either previously stored, or a real time stream), and creates a dynamic temporal graph that is partitioned over multiple machines. In addition to maintaining this model, graph analysis functions can be defined that will be executed across the cluster nodes, and will have access to the full history of a graph. Raphtory is designed with extensibility in mind; new types of data, as well as new analysis algorithms can be added to the base project, in order to support additional use cases.

An overview of this can be seen in the diagram below:

<p align="center">
  <img src="https://raphtory.github.io/images/overview.png" alt="Raphtory diagram"/>
</p>

The most recent article on Raphtory can be found [here](https://www.sciencedirect.com/science/article/pii/S0167739X19301621). As an example of the sort of analysis which can be conducted with Raphtory, checkout our paper on the social network Gab.ai [here.](https://arxiv.org/pdf/2009.08322.pdf). For an introduction to temporal graphs and to see some of the exciting software being built on top of Raphtory, check out the work by [Chorograph](https://chorograph.com/demo).

# Documentation & Slack
The documentation on how to run Raphtory and an in-depth look at the sort of analysis it performs can be found at [https://raphtory.github.io/](https://raphtory.github.io/images/overview.png).

If you have any queries on running Raphtory, or want to get involved with the development, please join our [slack](https://join.slack.com/t/raphtory/shared_invite/zt-jd5mce91-vDxEiFBILC_G2ilZPdvDaA). Feel free to join the #raphtory-development channel to discuss current issues, ask your questions in #general or ping Ben Steer (miratepuffin) who can give you a hand.


