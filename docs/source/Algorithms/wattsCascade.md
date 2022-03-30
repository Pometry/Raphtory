# Watts Cascade
---

This algorithm will run the Watts Cascade. 
This algorithm, presented by Duncan Watts in 2002, 
presents a method for the spread of "infectious ideas."
In the model, people are represented as nodes and relationships are edges. 
Each node is given a random or deterministic threshold that states
it will accept an idea if the threshold of its neighbours accepting the 
idea has exceeded. 


### Parameters

* `infectedSeed` _(Array(Long))_ : The list of node IDs to that begin infection. 
* `UNIFORM_RANDOM` _(Int)_ : If equal to threshold choice, then all thresholds will be set at random.
* `threshold_choice` _(Double)_ : Default threshold to trigger change of state.
* `seed` _(Int)_ : Value used for the random selection, can be set to ensure same result is returned per run. If not specified, it will generate a random seed.
* `output` _(String)_ : The path where the output will be saved. If not specified, defaults to `/tmp/wattsCascade`.

## Implementation

1. In the first step the state of all nodes are set. This includes whether they are initially infected and their threshold. 

2. Each non-infected vertex checks whether the number of infected messages it has received outweighs its threshold, if so then
it sets its state to be infected and then annouces this to all of its neighbours.

### Returns
* `Node ID` _(Long)_: Vertex ID 
* `Name of vertex`_(String)_:  `name` property of vertex if set
* `Infected` _(Boolean)_:  True or False, whether node was infected or not
