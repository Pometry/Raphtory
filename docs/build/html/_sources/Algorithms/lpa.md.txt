# Label Propagation Algorithm
---


<p style="margin-left: 1.5em;"> Returns the communities of the constructed graph as detected by synchronous label propagation.</p>

Every vertex is assigned an initial label at random. Looking at the labels of its neighbours, a probability is assigned to observed labels following an increasing function then the vertex's label is updated with the label with the highest probability. If the new label is the same as the current label, the vertex votes to halt. This process iterates until all vertice labels have converged. The algorithm is synchronous since every vertex updates its label at the same time.


### Parameters
* `weight`  _(String)_  --  Edge property (_Default: `""`_). To be specified in case of weighted graph.
* `cutoff`  _(Double)_  --  Outlier score threshold. (_Default: 0.0_). 
              Returns the outliers with an outlier score > `cutoff`.
* `maxIter` _(Int)_ --  Maximum iterations for algorithm to run. (_Default: 500_)
* `seed` _(Long)_ -- Indicator for randomness state. (_Default: -1_)
* `output_dir` _(String)_ -- Directory path where the output is written to. (_Default:_ `"/tmp/LPA"`)


### Returns
* `ID` _(Long)_ : Vertex ID
* `Label` _(Long)_ : The ID of the community this belongs to.


#### Notes
* This implementation of LPA incorporates probabilistic elements which makes it non-deterministic; The returned communities may differ on multiple executions. 

#### See also
<button onclick="location.href='cbod'" type="button" class="btn btn-default">
         Outlier Detection</button>
         
<button onclick="location.href='mlpa'" type="button" class="btn btn-default">
         Multi-Layer Dynamic Community Detection</button>

## Examples
In this example, the temporal network spans a time period $t \in [1,3]$ and is built into (3) snapshots of window size 1.

<p align="center">
  <img src="../_static/lpa-ex.png" style="width: 30vw;" alt="lpa example"/>
</p>


Running LPA on all snapshots;
```scala
RG.rangeQuery(LPA(), start = 1,end = 3,increment = 1,windows = List(1))
```

This returns:

```json
1,1,1,4042622
1,1,2,4042622
1,1,5,4042622
1,1,6,4042622
2,1,1,5545750
2,1,2,5545750
2,1,3,5545750
2,1,4,5545750
2,1,5,3223962
2,1,6,3223962
3,1,1,679956
3,1,3,679956
3,1,4,679956
3,1,5,1206247
3,1,6,1206247
3,1,7,1206247
```
Notice that the 1st column represents the time point where the query is run and the 2nd column is reporting the window size.

Running LPA on the aggregated graph;
```scala
RG.pointQuery(LPA(),timestamp = 3)
```

This returns:

```json
3,1,8858
3,2,8858
3,3,8858
3,4,8858
3,5,6954665
3,6,6954665
3,7,6954665
```