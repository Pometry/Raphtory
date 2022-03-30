# Multi-Layer Dynamic Community Detection
---

<p style="margin-left: 1.5em;"> Returns the communities of a multi-layer graph as detected by synchronous label propagation.</p>

  This returns the communities of the constructed multi-layer graph as detected by synchronous label propagation.

  This transforms the graph into a multi-layer graph where the same vertices on different layers are handled as
  distinct vertices. The algorithm then runs a version of LPA on this view of the graph and returns communities that
  share the same label that can span both vertices on the same layer and other layers.

### Parameters
  * `top` __(Int)__       : The number of top largest communities to return. (default: 0)
                        If not specified, Raphtory will return all detected communities.
  * `weight` __(String)__     : Edge property (default: ""). To be specified in case of weighted graph.
  * `maxIter` __(Int)__       : Maximum iterations for LPA to run. (default: 500)
  * `layers` __(List(Long))__ : List of layer timestamps.
  * `layerSize` __(Long)__    : Size of a single layer that spans all events occurring within this period.
  * `omega` __(Double)__      : Weight of temporal edge that are created between two layers for two persisting instances of a node.
                        (Default: 1.0) If "-1", the weights are assigned based on an average of the neighborhood of two layers.
  * `seed` __(Long)__         : Seed for random elements. (Default: -1).
  * `output` __(String)__     : Directory path that points to where to store the results. (Default: "/tmp/mLPA")

### Returns
* `total` _(Int)_ -- Number of detected communities.
* `communities` _(List[List[Long]])_ -- Communities sorted by their sizes where every member is in the form `vID_layerID`. Returns largest `top` communities if specified.


### Notes
  This implementation is based on LPA, which incorporates probabilistic elements. This makes it non-deterministic i.e. The returned communities may differ on multiple executions.



#### See also
<button onclick="location.href='lpa'" type="button" class="btn btn-default">
         Label Propagation Algorithm</button>

## Examples
The temporal network in the figure is a weighted dynamic graph spanning a time period $t \in [1,3]$ with edge property `weight`; and is built into (3) snapshots of window size 1.
 
<p align="center">
	<img src="../_static/mlpa-eg.png" style="width: 30vw;" alt="mlpa example"/>
</p>


Running the algorithm with a weak inter-layer coupling ($\omega = 1$);

Returns communities of every layer as if the layers are disconnected: 

```json
{"time":3,"top5":[4,3,2],"total":3,"totalIslands":0,"communities": [["3_3","4_3","1_3","2_3"],["3_1","1_1","2_1"],["1_2","2_2"]], "viewTime":47}
```
