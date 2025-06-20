# Introduction and dataset

After reading data into Raphtory we can now make use of the graph representation to ask some interesting questions. This example will use a dataset from [SocioPatterns](http://www.sociopatterns.org/datasets/baboons-interactions/), comprising different behavioral interactions between a group of 22 baboons over a month. 

!!! info 
    If you want to read more about the dataset, you can check it out in this paper: [V. Gelardi, J. Godard, D. Paleressompoulle, N. Claidière, A. Barrat, “Measuring social networks in primates: wearable sensors vs. direct observations”, Proc. R. Soc. A 476:20190737 (2020)](https://royalsocietypublishing.org/doi/10.1098/rspa.2019.0737). 

In the below code loads this dataset into a dataframe and does a small amount of preprocessing to prepare it for loading into Raphtory. This includes dropping rows with blank fields and mapping the values of the `behavior category` into a `weight` which can be aggregated. The mapping consists of the following conversions:

- Affiliative (positive interaction) → `+1`
- Agonistic (negative interaction) → `-1` 
- Other (neutral interaction) → `0`

/// tab | :fontawesome-brands-python: Python

```python
from raphtory import Graph
from datetime import datetime
import pandas as pd

edges_df = pd.read_csv(
    "../data/OBS_data.txt", sep="\t", header=0, usecols=[0, 1, 2, 3, 4], parse_dates=[0]
)
edges_df["DateTime"] = pd.to_datetime(edges_df["DateTime"])
edges_df.dropna(axis=0, inplace=True)
edges_df["Weight"] = edges_df["Category"].apply(
    lambda c: 1 if (c == "Affiliative") else (-1 if (c == "Agonistic") else 0)
)
print(edges_df.head())
```
///

!!! Output

    ```output
                  DateTime   Actor Recipient  Behavior     Category  Weight
    15 2019-06-13 09:50:00  ANGELE    FELIPE  Grooming  Affiliative       1
    17 2019-06-13 09:50:00  ANGELE    FELIPE  Grooming  Affiliative       1
    19 2019-06-13 09:51:00  FELIPE    ANGELE   Resting  Affiliative       1
    20 2019-06-13 09:51:00  FELIPE      LIPS   Resting  Affiliative       1
    21 2019-06-13 09:51:00  ANGELE    FELIPE  Grooming  Affiliative       1
    ```

Next we load this into Raphtory using the `load_edges_from_pandas` function, modelling it as a weighted multi-layer graph, with a layer per unique `behavior`. 

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
import raphtory as rp

g = rp.Graph()
g.load_edges_from_pandas(
    df=edges_df,
    src="Actor",
    dst="Recipient",
    time="DateTime",
    layer_col="Behavior",
    properties=["Weight"],
)
print(g)
```
///

!!! Output

    ```output
    Graph(number_of_nodes=22, number_of_edges=290, number_of_temporal_edges=3196, earliest_time=1560419400000, latest_time=1562756700000)
    ```
 