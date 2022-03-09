`com.raphtory.algorithms.generic.centrality.Distinctiveness`
(com.raphtory.algorithms.generic.centrality.Distinctiveness)=
# Distinctiveness

{s}`Distinctiveness(alpha: Double=1.0, weightProperty="weight")`
 : compute distinctiveness centralities of nodes

Distinctiveness centrality measures importance of a node through how it
bridges different parts of the graph, approximating this by connections to
low degree nodes. For more info read
[Distinctiveness centrality in social networks](https://journals.plos.org/plosone/article/file?id=10.1371/journal.pone.0233276&type=printable)

```{note}
The network is treated as undirected.
```

## Parameters

 {s}`alpha: Double = 1.0`
   : tuning exponent

 {s}`weightProperty: String = "weight"`
   : name of property to use for edge weight. If not found, edge weight is treated as number of edge occurrences.

## States

 {s}`D1: Double`, ... , {s}`D5: Double`
   : versions of distinctiveness centrality

## Returns

 | vertex name       | D1              | ... | D5              |
 | ----------------- | --------------- | --- | --------------- |
 | {s}`name: String` | {s}`D1: Double` | ... | {s}`D5: Double` |