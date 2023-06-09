---
title: 'Raphtory: a library for mining large temporal graphs'
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
  - name: Haaroon Yousaf
    affiliation: 1
  - name: Renaud Lambiotte:
    affiliation: 2
affiliations:
 - name: Pometry, United Kingdom
   index: 1
 - name: Mathematical Institute, University of Oxford, United Kingdom
   index: 2
 - name: Networks Science Institute, Northeastern University London, United Kingdom
   index: 3
 - name: School of Electronic Engineering and Computer Science, Queen Mary University of London, United Kingdom
   index: 4
date: 13 August 2017
bibliography: joss-raphtory.bib

# # Optional fields if submitting to a AAS journal too, see this blog post:
# # https://blog.joss.theoj.org/2018/12/a-new-collaboration-with-aas-publishing
# aas-doi: 10.3847/xxxxx <- update this with the DOI from AAS once you know it.
# aas-journal: Astrophysical Journal <- The name of the AAS journal.
---

# Summary

Raphtory is an analytics platform which combines the capabilities of a graph structure with time-series analysis, providing a powerful model for extracting new insights from temporal networks. Temporal networks generalise standard networks by allowing its edges and nodes to be dynamical entities, and not static ones. The library provides methods to build temporal networks from empirical data and to manipulate them, as well as algorithms to explore their structure and evolution. Raphotry is built in Rust, for its efficiency, with a Python interface, for its ease of use. Raphtory is developed by network scientists, with a background in Physics, Applied Mathematics, Engineering  and Computer Science, for network scientists. 

# Statement of need

Networks are at the core of data science solutions in a variety of domains, including computer science, computational social science, and the life sciences \cite{newman2018networks}. Networks are a powerful language focusing on the connectivity of systems, and offering a rich toolbox to extract information in relational data. Several packages exist and make its tools accessible to scientists, practitioners and data miners, including NetworkX [@hagberg2008exploring], graph-tool[@peixoto2014graph]  and igraph [@csardi2006igraph]. 
In recent years, relational data have become increasingly rich, combining different types of information that may not be incorporated in a standard network model [@lambiotte2019networks]. In particular, the temporal nature of many networked systems has led to the emergence of the field of temporal networks, with its own models and algorithms [@holme2012temporal;@masuda2016guide].
However, despite an active research activity in the last decade, and the development of several methods, no efficient package allows to manipulate and to explore the temporal dimension of networks. To support researchers confronted to temporal relational data in academia and in the industry, we have developed Raphtory, an open-source solution in Rust with a Python interface.

# Related Software

Besides the packages for static networks aforementioned, few open access tools have been developed for the mining of temporal networks, yet the existing solutions tend to be in a preliminary state, or focusing on specific questions.
For instance, EvolvingGraphs.jl only proposes a small number of algorithms and its development has been interrupted 5 years ago [@zhang2015dynamic]. pathpy is a Python package for the analysis of time series data on networks adopting the viewpoint of multi-order network models [@hackl2021analysis]. More recently, but in a relatively preliminary form, Reticula offers a range of methods developed in C++ with a Python interface [@badie2023reticula].
Raphtory is a valuable addition to this ecosystem for the following reasons. Originally developed in Scala [@steer2020raphtory], its current core is entirely written in Rust, to ensure smooth distributed computations together with a Python interface for the facility of use. The distributed setting is particularly critical, to handle the sheer volume of temporal network data, whose size is often significantly larger than that of their static counterparts.

# Overview

The installation is run with a single `pip install raphtory` command, facilitating its fit into a Python/Pandas pipeline for input and output.

# Projects using Raphtory

Raphtory has proved an invaluable resource in industrial and academic   projects, for instance to characterise the time evolution of the fringe social network Gab [@arnold2021moving], transactions of users of a dark web marketplace Alphabay using temporal motifs [@paranjape2017motifs] or anomalous patterns of activity in NFT trades [@yousaf2023non]. The library has recently been significantly rewritten, and we expect that with its new functionalities, efficiency and ease of use, it will become an essential part of the network scince community.