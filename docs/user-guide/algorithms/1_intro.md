# Running algorithms 

Raphtory implements many of the standard algorithms you expect within a graph library, but also has several temporal algorithms such as `Temporal Reachability` and `Temporal Motifs`. 

Raphtory categorizes algorithms into `graphwide` and `node centric` algorithms.

- `graphwide`: returns one value for the whole graph. 

- `node centric`: returns one value for each node in the graph

For these examples we are going to use the [One graph to rule them all](https://arxiv.org/abs/2210.07871) dataset, which maps the co-occurrence of characters in the Lord of The Rings books. 

This dataset is a simple edge list, consisting of the source character, destination character and the sentence they occurred together in (which we use as a timestamp). The dataframe for this can be seen in the output below.

=== ":fontawesome-brands-python: Python"

    ```python
    from raphtory import Graph
    import pandas as pd

    df = pd.read_csv("docs/data/lotr.csv")
    print(df)

    lotr_graph = Graph()
    lotr_graph.load_edges_from_pandas(
        df=df,time="time", src="src", dst="dst"
    )
    ```

!!! Output

    ```output
                src        dst   time
    0       Gandalf     Elrond     33
    1         Frodo      Bilbo    114
    2        Blanco     Marcho    146
    3         Frodo      Bilbo    205
    4        Thorin    Gandalf    270
    ...         ...        ...    ...
    2644      Merry  Galadriel  32666
    2645      Merry        Sam  32666
    2646  Galadriel        Sam  32666
    2647     Pippin      Merry  32671
    2648     Pippin      Merry  32674

    [2649 rows x 3 columns]
    ```
