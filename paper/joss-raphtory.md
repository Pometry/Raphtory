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

# Summary

Raphtory is a platform for building and analysing temporal networks. The library includes methods for creating networks from a variety of data sources; algorithms to explore their structure and evolution; and an extensible GraphQL server for deployment of applications built on top. Raphtory's core engine is built in Rust, for efficiency, with Python interfaces, for ease of use. Raphtory is developed by network scientists, with a background in Physics, Applied Mathematics, Engineering and Computer Science, for use across academia and industry.


# Statement of need

Networks are at the core of data science solutions in a variety of domains, including computer science, computational social science, and the life sciences [@newman2018networks]. Networks are a powerful language focusing on the connectivity of systems, and offer a rich toolbox to extract greater understanding from data. Several network analysis tools exist, including NetworkX [@hagberg2008exploring], graph-tool[@peixoto2014graph]  and igraph [@csardi2006igraph], and are freely accessible to scientists, practitioners and data miners.

However, with abundent cheap storage and tools for logging every event which occurs in an ecosystem, datasets have become increasingly rich, combining different types of information that cannot be incorporated in a standard network model [@lambiotte2019networks]. In particular, the temporal nature of many complex systems has led to the emergence of the field of temporal networks, with its own models and algorithms [@holme2012temporal;@masuda2016guide].

Unfortuntely, despite active research in the last decade, no efficient, generalised and production-ready system has been developed to explore the temporal dimension of networks. To support practitioners who wish to exploit both the structure and dynamics of their data, we have developed Raphtory.

# Related Software

Besides the aforementioned packages, few open access tools have been developed for the mining of temporal networks, with the existing solutions focusing on specific sub-problems within the space. Those which have attempted to generalise to all temporal network analysis are either actively under development, but too perliminary to use in production, or have been abandonded due to lack of funding/changing research goals.

As examples of these three catagories: Pathpy is a Python package for the analysis of time series data on networks, but focuses on extracting and analysing time-respecting paths [@hackl2021analysis]. The recently released Reticula offers a range of methods developed in C++ with a Python interface [@badie2023reticula]. EvolvingGraphs.jl[@zhang2015dynamic], RecallGraph[@RecallGraph] and Chronograph[@Chronograph] all saw significant work before development was halted indefinitely.


**DRAFT FROM HERE DOWN**

Raphtory is a valuable addition to this ecosystem for the following reasons. Originally developed in Scala [@steer2020raphtory], its current core is entirely written in Rust. This is to ensure fast and memory-effecient computation, and to handle the sheer volume of temporal network data, which often dwarfs that of than an equivilant static network.

together with a Python interface for the facility of use.

# Overview

The core objects in Raphtory are the Graph and GraphView objects. The Graph object maintains a chronological log of all vertices, edges and their properties over time. A graph can be created using simple functions for adding vertices and edges at different time points as well as updating their properties, or through in-built loaders for common data formats. Then, the GraphView object provides a view of the graph according to a temporal or structural scope, and can be queried in the same way as the Graph object. Accompanying this is functionality for generating GraphViews programmatically over a desired time range. For example, given a dataset of social network interactions, one might generate a daily, weekly, monthly rolling window view of the graph containing all interactions within that month.

As well as applying traditional graph algorithms or metrics to different views of the graph over time, one can exp

![Number of nodes over](https://hackmd.io/_uploads/BkS5uRgP2.png) ![](https://hackmd.io/_uploads/BJgo_CxP3.png) ![](https://hackmd.io/_uploads/HJb3uAgv2.png)

The installation is run with a single `pip install raphtory` command, facilitating its fit into a Python/Pandas pipeline for input and output.

# Projects using Raphtory

Raphtory has proved an invaluable resource in industrial and academic projects, for instance to characterise the time evolution of the fringe social network Gab [@arnold2021moving], transactions of users of a dark web marketplace Alphabay using temporal motifs [@paranjape2017motifs] or anomalous patterns of activity in NFT trades [@yousaf2023non]. The library has recently been significantly rewritten, and we expect that with its new functionalities, efficiency and ease of use, it will become an essential part of the network scince community.
