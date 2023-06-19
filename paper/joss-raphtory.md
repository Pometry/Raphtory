---
title: 'Raphtory: The temporal graph engine for Rust and Python'
tags:
  - Python
  - Rust
  - temporal networks
  - graphs
  - dynamics
authors:
  - name: Ben Steer
    orcid: 0000-0000-0000-0000
    affiliation: "1, 4" # (Multiple affiliations must be quoted)
    corresponding: true # (This is how to denote the corresponding author)
  - name: Naomi Arnold
    affiliation: 3
  - name: Renaud Lambiotte
    affiliation: "2, 1"
  - name: Haaroon Yousaf
    affiliation: 1
  - name: Lucas Jeub
    affiliation: 1
  - name: Fabian Murariu
    affiliation: 5
  - name: Shivam Kapoor
    affiliation: 1
  - name: Pedro Rico
    affiliation: 1
  - name: Rachel Chan
    affiliation: 1
  - name: Louis Chan
    affiliation: 1
  - name: James Alford
    affiliation: 1
  - name: Richard G. Clegg
    affiliation: 4
  - name: Felix Cuadrado
    affiliation: "7, 4"
  - name: Matt Barnes
    affiliation: 4
  - name: Peijie Zhong
    affiliation: 4
  - name: Cheick Tidiane Ba
    affiliation: "6, 4"
  - name: John Pougué-Biyong
    affiliation: 2
  - name: Alhamza Alnaimi
    affiliation: 1

affiliations:
 - name: Pometry, United Kingdom
   index: 1
 - name: Mathematical Institute, University of Oxford, United Kingdom
   index: 2
 - name: Networks Science Institute, Northeastern University London, United Kingdom
   index: 3
 - name: School of Electronic Engineering and Computer Science, Queen Mary University of London, United Kingdom
   index: 4
 - name: 32 Bytes Software, United Kingdom
   index: 5
 - name: University of Milan, Italy
   index: 6
 - name: Universidad Politécnica de Madrid, Spain
   index: 7
date: 09 July 2023 
bibliography: joss-raphtory.bib

# # Optional fields if submitting to a AAS journal too, see this blog post:
# # https://blog.joss.theoj.org/2018/12/a-new-collaboration-with-aas-publishing
# aas-doi: 10.3847/xxxxx <- update this with the DOI from AAS once you know it.
# aas-journal: Astrophysical Journal <- The name of the AAS journal.
---
# Raphtory: The temporal graph engine for Rust and Python

# Summary

Raphtory is a platform for building and analysing temporal networks. The library includes methods for creating networks from a variety of data sources; algorithms to explore their structure and evolution; and an extensible GraphQL server for deployment of applications built on top. Raphtory's core engine is built in Rust, for efficiency, with Python interfaces, for ease of use. Raphtory is developed by network scientists, with a background in Physics, Applied Mathematics, Engineering and Computer Science, for use across academia and industry. 


# Statement of need

Networks are at the core of data science solutions in a variety of domains, including computer science, computational social science, and the life sciences [@newman2018networks]. Networks are a powerful language focusing on the connectivity of systems, and offer a rich toolbox to extract greater understanding from data. Several network analysis tools exist, including NetworkX [@hagberg2008exploring], graph-tool [@peixoto2014graph]  and igraph [@csardi2006igraph], and are freely accessible to scientists, practitioners and data miners. 

However, with abundant cheap storage and tools for logging every event which occurs in an ecosystem, datasets have become increasingly rich, combining different types of information that cannot be incorporated in a standard network model [@lambiotte2019networks]. In particular, the temporal nature of many complex systems has led to the emergence of the field of temporal networks, with its own models and algorithms [@holme2012temporal;@masuda2016guide].

Unfortuntely, despite active academic research in the last decade, no efficient, generalised and production-ready system has been developed to explore the temporal dimension of networks. To support practitioners who wish to exploit both the structure and dynamics of their data, we have developed Raphtory.

# Related Software

Besides the aforementioned packages, few open access tools have been developed for the mining of temporal networks, with the existing solutions focusing on specific sub-problems within the space. Those which have attempted to generalise to all temporal network analysis are either actively under development, but too preliminary to use in production, or have been abandoned due to lack of funding or changing research goals. 

As examples of these three catagories: Pathpy is a Python package for the analysis of time series data on networks, but focuses on extracting and analysing time-respecting paths [@hackl2021analysis], and similarly DyNetX [@DyNetX], a pure python library relying on networkX, focuses on temporal slicing and the computation of time-respecting paths. The recently released Reticula offers a range of methods developed in C++ with a Python interface [@badie2023reticula]. EvolvingGraphs.jl [@zhang2015dynamic], RecallGraph [@RecallGraph] and Chronograph [@Chronograph] all saw significant work before development was halted indefinitely.


Raphtory is a valuable addition to this ecosystem for the following reasons. Originally developed in Scala [@steer2020raphtory], its current core is entirely written in Rust. This is to ensure fast and memory-effecient computation that a pure python implementation could not achieve, and to handle the sheer volume of temporal network data, which often dwarfs that of an equivalent static network.


The library provides an expressive Python interface for facility of use, as well as simpler and more maintainable code. In addition, the library is built with a focus on scalability, as it relies on efficient data structures that can be used to extract different views of large temporal graphs. This avoids the creation of multiple graph objects that is not feasible with large datasets. The use of these new features is supported by well-documented APIs and tutorials, guiding the user from data loading up to analysis.


# Overview

The core objects in Raphtory are the Graph and GraphView objects. The Graph object maintains a chronological log of all vertices, edges and their properties over time. A graph can be created using simple functions for adding vertices and edges at different time points as well as updating their properties, or through in-built loaders for common data (Example 1). 

Then, the GraphView object provides a view of the underlying temporal graph, according to a temporal or structural scope, and can be queried in the same way as the Graph object. GraphViews can be generated programmatically over a desired time range (windows) and over a subset of layers if the graph is multilayer. Additionally, the views can leverage event durations and support the modeling of deletion operations. These views can be explored and analysed through the application of traditional graph algorithms and metrics (Example 2). 

Raphtory provides fast and scalable implementation of algorithms for temporal network mining such as temporal motifs (Example 3) and temporal reachability. Moreover, Raphtory is equipped with further utilities for working with temporal data, including null model generation, a time module allowing the use of string dates as well as unix timestamps.<!--, and ... ?-->

Finally, Rapthory is built with a focus on the ease of use. The installation is run with a single `pip install raphtory` command, facilitating its fit into a Python/Pandas pipeline for input and output. In addition, Raphtory APIs should facilitate the path for contribution to the evergrowing selection of availble algorithms. <!-- The history of entities should be the starting block to derive the temporal version of the most common network algorithms. -->

 Example code             |  Visualisation
:-------------------------:|:-------------------------:
![](https://hackmd.io/_uploads/Bk5MI2Fw2.png)|![](https://hackmd.io/_uploads/BJhzditwn.png)
![](https://hackmd.io/_uploads/r1RKkp6v3.png)|![](https://hackmd.io/_uploads/BJ-qL2Yvh.png)
![](https://hackmd.io/_uploads/S1mg_UvP2.png) | ![](https://hackmd.io/_uploads/HJb3uAgv2.png)
: First line (Example 1): In a temporal network, edges are dynamical entities connecting pairs of nodes at specific times and, possibly, over specific time windows. Second line (Example 2): Raphtory swiftly allows to generate sequences of graphs at a given time resolution, and to run standard network algorithms, here Pagerank. Third line (Example 3): Raphtory also offers rapid implementations of algorithms specifically designed for temporal networks, here counting and estimating the significance of temporal motifs (maybe add a link to the paper of Leskovec, or the one of Naomi?[@paranjape2017motifs]?). For each example, we provide the Python code required to perform the operation.

<!-- ![](https://hackmd.io/_uploads/Bk5MI2Fw2.png)-->


# Projects using Raphtory

Raphtory has proved an invaluable resource in industrial and academic projects, for instance to characterise the time evolution of the fringe social network Gab [@arnold2021moving], transactions of users of a dark web marketplace Alphabay using temporal motifs [@paranjape2017motifs] or anomalous patterns of activity in NFT trades [@yousaf2023non]. The library has recently been significantly rewritten, and we expect that with its new functionalities, efficiency and ease of use, it will become an essential part of the network science community.
