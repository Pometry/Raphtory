# Raphtory Storage

How graph updates are written into the in-memory segments and made ready for reads and flushes.

## Writes

**Bulk load**:
A load of a whole dataframe that holds the write locks on every segment for its full duration and may leave per-entity time data unordered until it finishes.
_Avoid_: Batch load, import

**Online write**:
A single update arriving through the graph API or WAL replay, which leaves every time structure ordered after it is applied.
_Avoid_: Live write, incremental load

**Finishing a bulk load**:
The point at which every time structure and adjacency list a bulk load left unordered is put back in order, before the locks are released. It happens whether the load succeeded or failed; outside a bulk load nothing is ever unordered.
_Avoid_: End of batch, finalise

## Time structures

**Reverse time index**:
A per-layer index from update time to the rows updated at that time, kept separately for property updates and for edge events. One time can map to several rows.
_Avoid_: Time rows, time index (that is the on-disk structure built at flush)
