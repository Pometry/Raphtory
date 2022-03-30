# Tainting / Infection
---
A tainting/infection algorithm for a directed graph. Given the start node(s) and time
this algorithm will spread taint/infect any node which has received an edge after this time.
The following node will then infect any of its neighbours that receive an edge after the
time it was first infected. This repeats until the latest timestamp/iterations are met.

## Parameters 

*  `startTime` __(Long)__ : Time to start spreading taint
*  `infectedNodes` __(Set[Long])__ : List of nodes that will start as tainted
*  `stopNodes` __(Set[Long])__ : If set, any nodes that will not propogate taint
*  `output` __(String)__ : Path where the output will be written


## Returns

* `Infected ID` __(Long)__ : ID of the infected vertex
* `Edge ID` __(Long)__ : Edge that propogated the infection
* `Time` __(Long)__ : Time of infection
* `Infector ID` __(Long)__ : Node that spread the infection

