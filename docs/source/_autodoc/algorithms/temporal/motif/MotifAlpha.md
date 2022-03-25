`com.raphtory.algorithms.temporal.motif.MotifAlpha`
(com.raphtory.algorithms.temporal.motif.MotifAlpha)=
# MotifAlpha

{s}`MotifAlpha()`
 : count Type1 2-edge-1-node temporal motifs

The algorithms identifies 2-edge-1-node temporal motifs; It detects one type of motifs:
For each incoming edge a vertex has, the algorithm checks whether there are any outgoing
edges which occur after it and returns a count of these.

````{note}
In other words, it detects motifs that exhibit incoming flow followed by outgoing flow in the form

```{image} /images/mc1.png
:width: 200px
:alt: mc type 1
:align: center
```
````

## States

 {s}`motifAlpha: Int`
   : Number of Type-1 temporal motifs centered on the vertex

## Returns

 | vertex name       | Number of Type-1 motifs |
 | ----------------- | ----------------------- |
 | {s}`name: String` | {s}`motifAlpha: Int`    |