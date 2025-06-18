# Running algorithms 

Raphtory implements many of the standard algorithms you expect within a graph library, but also has several temporal algorithms such as `Temporal Reachability` and `Temporal Motifs`. 

Raphtory categorizes algorithms into `graphwide` and `node centric` algorithms.

- `graphwide`: returns one value for the whole graph. 

- `node centric`: returns one value for each node in the graph

For these examples we are going to use the [One graph to rule them all](https://arxiv.org/abs/2210.07871) dataset, which maps the co-occurrence of characters in the Lord of The Rings books. 

This dataset is a simple edge list, consisting of the source character, destination character and the sentence they occurred together in (which we use as a timestamp). The dataframe for this can be seen in the output below.

{{code_block('getting-started/algorithms','data_loading',['Graph'])}}
!!! Output

    ```python exec="on" result="text" session="algorithms"
    --8<-- "python/getting-started/algorithms.py:data_loading"
    ```
