`com.raphtory.algorithms.generic.motif.ThreeNodeMotifs`
(com.raphtory.algorithms.generic.motif.ThreeNodeMotifs)=
# ThreeNodeMotifs

 {s}`ThreeNodeMotifs()`
   : Count occurrences of three-node motifs that a node participates in.

 The algorithm works by first counting star motifs, including potential triangles.
 Then, for each star we send a message to one of the neighbours to identify the potential third edge.
 Based on the responses, we correct the star counts and count the triangles.

 ```{note}
 Stars are only counted once as they only appear in the counts for the central vertex. However, triangles
 appear for each of the 3 vertices, so the count for triangles should be divided by 3 when aggregating results to get
 motif counts for the whole graph.
 ```

 ## Motifs

 ### Stars

   - 0: l <- c -> r
   - 1: l -> c <- r
   - 2: l -> c -> r
   - 3: l -> c <-> r
   - 4: l <- c <-> r
   - 5: l <-> c <-> r

 ### Triangles

 (Index of star + third edge)
   - 6: 0 + l -> r
   - 7: 2 + l <- r
   - 8: 3 + l -> r
   - 9: 4 + l -> r
   - 10: 4 + l <- r
   - 11: 5 + l -> r
   - 12: 5 + l <-> r

## States
 {s}`motifCounts: Array[Long]`
   : Motif counts stored as an array (see indices above)

## Returns

 | vertex name       | Motif 0                   | ... | Motif 12                   |
 | ----------------- | ------------------------- | --- | -------------------------- |
 | {s}`name: String` | {s}`motifCounts(0): Long` | ... | {s}`motifCounts(12): Long` |