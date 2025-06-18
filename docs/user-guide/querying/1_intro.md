# Introduction and dataset

After reading data into Raphtory we can now make use of the graph representation to ask some interesting questions. This example will use a dataset from [SocioPatterns](http://www.sociopatterns.org/datasets/baboons-interactions/), comprising different behavioral interactions between a group of 22 baboons over a month. 

!!! info 
    If you want to read more about the dataset, you can check it out in this paper: [V. Gelardi, J. Godard, D. Paleressompoulle, N. Claidière, A. Barrat, “Measuring social networks in primates: wearable sensors vs. direct observations”, Proc. R. Soc. A 476:20190737 (2020)](https://royalsocietypublishing.org/doi/10.1098/rspa.2019.0737). 

In the below code loads this dataset into a dataframe and does a small amount of preprocessing to prepare it for loading into Raphtory. This includes dropping rows with blank fields and mapping the values of the `behavior category` into a `weight` which can be aggregated. The mapping consists of the following conversions:

- Affiliative (positive interaction) → `+1`
- Agonistic (negative interaction) → `-1` 
- Other (neutral interaction) → `0`

{{code_block('getting-started/querying','read_data',['Graph'])}}
!!! Output

    ```python exec="on" result="text" session="getting-started/querying"
    --8<-- "python/getting-started/querying.py:read_data"
    ```

Next we load this into Raphtory using the `load_edges_from_pandas` function, modelling it as a weighted multi-layer graph, with a layer per unique `behavior`. 

{{code_block('getting-started/querying','new_graph',['Graph'])}}
!!! Output

    ```python exec="on" result="text" session="getting-started/querying"
    --8<-- "python/getting-started/querying.py:new_graph"
    ```
 