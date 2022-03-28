`com.raphtory.core.algorithm.GraphPerspective`
(com.raphtory.core.algorithm.GraphPerspective)=
# GraphPerspective

{s}`GraphPerspective`
 : Public interface for graph algorithms

The {s}`GraphPerspective` is the core interface of the algorithm API and records the different graph operations
to execute.

## Methods

 {s}`setGlobalState(f: (GraphState) => Unit): GraphPerspective`
   : Add a function to manipulate global graph state, mainly used to initialise accumulators
      before the next algorithm step

     {s}`f: (GraphState) => Unit`
     : function to set graph state (run exactly once)

 {s}`filter(f: (Vertex) => Boolean): GraphPerspective`
   : Filter nodes

     {s}`f: (Vertex) => Boolean)`
     : filter function (only vertices for which {s}`f` returns {s}`true` are kept)

 {s}`filter(f: (Vertex, GraphState) => Boolean): GraphPerspective`
   : Filter nodes with global graph state

     {s}`f: (Vertex, GraphState) => Boolean`
       : filter function with access to graph state (only vertices for which {s}`f` returns {s}`true` are kept)

 {s}`step(f: (Vertex) => Unit): GraphPerspective`
   : Execute algorithm step

     {s}`f: (Vertex) => Unit`
       : algorithm step (run once for each vertex)

 {s}`step(f: (Vertex, GraphState) => Unit): GraphPerspective`
   : Execute algorithm step with global graph state (has access to accumulated state from
     previous steps and allows for accumulation of new values)

     {s}`f: (Vertex, GraphState) => Unit`
       : algorithm step (run once for each vertex)

 {s}`iterate(f: (Vertex) => Unit, iterations: Int, executeMessagedOnly: Boolean): GraphPerspective`
    : Execute algorithm step repeatedly for given number of iterations or until all vertices have
      voted to halt.

      {s}`f: (Vertex) => Unit`
        : algorithm step (run once for each vertex per iteration)

      {s}`iterations: Int`
        : maximum number of iterations

      {s}`executeMessagedOnly: Boolean`
        : If {s}`true`, only run step for vertices which received new messages

  {s}`iterate(f: (Vertex, GraphState) => Unit, iterations: Int, executeMessagedOnly: Boolean): GraphPerspective`
    : Execute algorithm step with global graph state repeatedly for given number of iterations or
      until all vertices have voted to halt.

      {s}`f: (Vertex, GraphState) => Unit`
        : algorithm step (run once for each vertex per iteration)

      {s}`iterations: Int`
        : maximum number of iterations

      {s}`executeMessagedOnly: Boolean`
        : If {s}`true`, only run step for vertices which received new messages

 {s}`select(f: Vertex => Row): Table`
    : Write output to table

      {s}`f: Vertex => Row`
        : function to extract data from vertex (run once for each vertex)

 {s}`select(f: (Vertex, GraphState) => Row): Table`
    : Write output to table with access to global graph state

      {s}`f: (Vertex, GraphState) => Row`
        : function to extract data from vertex and graph state (run once for each vertex)

 {s}`globalSelect(f: GraphState => Row): Table`
    : Write global graph state to table (this creates a table with a single row)

      {s}`f: GraphState => Row`
        : function to extract data from graph state (run only once)

 {s}`nodeCount(): Int`
    : return number of nodes in the graph

```{seealso}
[](com.raphtory.core.algorithm.GraphState), [](com.raphtory.core.graph.visitor.Vertex)
```