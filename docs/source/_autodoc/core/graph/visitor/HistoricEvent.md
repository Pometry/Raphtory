`com.raphtory.core.graph.visitor.HistoricEvent`

(com.raphtory.core.graph.visitor.HistoricEvent)=
# HistoricEvent

{s}`HistoricEvent(time: Long, event: Boolean)`
 : Case class for encoding additions and deletions

This class is returned by the history access methods of [{s}`EntityVisitor`](com.raphtory.core.graph.visitor.EntityVisitor).
 ## Parameters

 {s}`time: Long`
   : timestamp of event

 {s}`event: Boolean`
   : {s}`true` if event is an addition or {s}`false` if event is a deletion

```{seealso}
[](com.raphtory.core.graph.visitor.EntityVisitor),
[](com.raphtory.core.graph.visitor.Vertex),
[](com.raphtory.core.graph.visitor.Edge)
`