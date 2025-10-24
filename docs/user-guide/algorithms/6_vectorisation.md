# Vectorisation

The [vectors][raphtory.vectors] module allows you to transform a graph into a collection of documents and vectorise those documents using an embedding function. Since the AI space moves quickly, Raphtory allows you to plug in your preferred embedding model either locally or from an API.

Using this you can perform [semantic search](https://en.wikipedia.org/wiki/Semantic_search) over your graph data and build powerful AI systems with graph based RAG.

## Vectorise a graph

To vectorise a graph you must create an embeddings function that takes a list of strings and returns a matching list of embeddings. This function can use any model or library you prefer, in this example we use the openai library and direct it to a local API compatible ollama service.

/// tab | :fontawesome-brands-python: Python
```{.python notest}
def get_embeddings(documents, model="embeddinggemma"):
    client = OpenAI(base_url='http://localhost:11434/v1/' api_key="ollama")
    return [client.embeddings.create(input=text, model=model).data[0].embedding for text in documents]

v = g.vectorise(get_embeddings, nodes=node_document, edges=edge_document, verbose=True)
```
///

When you call [Vectorise()][raphtory.GraphView.vectorise] Raphtory automatically creates documents for each node and edge entity in your graph, optionally you can create documents explicitly as properties and pass the property names to `vectorise()`. This is useful when you already have a deep understanding of your graphs semantics. Additionally, you can cache the embedded graph to disk to avoid having to recompute the vectors when nothing has changed.

## Retrieve documents

You can retrieve relevant information from the [VectorisedGraph][raphtory.vectors.VectorisedGraph] by making selections.

A [VectorSelection][raphtory.vectors.VectorSelection] is a general object for holding embedded documents, you can create an empty selection or perform a similarity query against a `VectorisedGraph` to populate a new selection.

You can add to a selection by combining existing selections or by adding new documents associated with specific nodes and edges by their IDs. Additionally, you can [expand][raphtory.vectors.VectorSelection.expand_entities_by_similarity] a selection by making similarity queries relative to the entities in the current selection, this uses the power of the graph relationships to constrain your query.

Once you have a selection containing the information you want you can:

- Get the associated graph entities using [nodes()][raphtory.vectors.VectorSelection.nodes] or [edges()][raphtory.vectors.VectorSelection.edges].
- Get the associated documents using [get_documents()][raphtory.vectors.VectorSelection.get_documents] or [get_documents_with_scores()][raphtory.vectors.VectorSelection.get_documents_with_scores].

Each [Document][raphtory.vectors.Document] corresponds to unique entity in the graph, the contents of the associated document and it's vector representation. You can pull any of these out to retrieve information about an entity for a RAG system, compose a subgraph to analyse using Raphtory's algorithms, or feed into some more complex pipeline.

## Example

