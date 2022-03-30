# Outlier Detection
---


<p style="margin-left: 1.5em;"> Returns outliers detected based on the community structure of the graph. </p>

  The algorithm detects nodes that don't have a particular strong connection to their assigned community and rather tend to connect multiple communities. The algorithm runs an instance of LPA on the graph, initially, and then defines an outlier score based on a node's community membership and how it compares to its neighbors community memberships.



### Parameters
* `weight`	_(String)_	-- 	Edge property (_Default: `""`_). To be specified in case of weighted graph.
* `cutoff`	_(Double)_	-- 	Outlier score threshold. (_Default: 0.0_). 
							Returns the outliers with an outlier score > `cutoff`.
* `maxIter`	_(Int)_	-- 	Maximum iterations for algorithm to run. (_Default: 500_)
* `seed` _(Long)_ -- Indicator for randomness state. (_Default: -1_)
* `output_dir` _(String)_ -- Directory path where the output is written to. (_Default:_ `"/tmp/CBOD"`)

### Returns
* `ID` _(Long)_ -- Vertex ID.
* `outlier_score`	_Double_ 	-- Outlier score for the vertex.

#### See also
<button onclick="location.href='/algorithms/lpa/'" type="button" class="btn btn-default">
         Label Propagation Algorithm</button>
<button onclick="location.href='/algorithms/mlpa/'" type="button" class="btn btn-default">
         Multi-Layer Dynamic Community Detection</button>