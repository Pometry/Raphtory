# Vectorisation

The [vectors][raphtory.vectors] module allows you to transform a graph into a collection of documents and vectorise those documents using an embedding function. Since the AI space moves quickly, Raphtory allows you to plug in your preferred embedding model either locally or from an API.

Using this you can perform [semantic search](https://en.wikipedia.org/wiki/Semantic_search) over your graph data and build powerful AI systems with graph based RAG.

## Vectorise a graph

To vectorise a graph you must create an embeddings function that takes a list of strings and returns a matching list of embeddings. This function can use any model or library you prefer, in this example we use the openai library and direct it to a local API compatible ollama service.

```python
def get_embeddings(documents, model="text-embedding-3-small"):
    client = OpenAI(base_url='http://localhost:11434/v1/' api_key="ollama")
    return [client.embeddings.create(input=text, model=model).data[0].embedding for text in documents]

v = g.vectorise(get_embeddings, nodes=node_document, edges=edge_document, verbose=True)
```

When you call [Vectorise()][raphtory.GraphView.vectorise] Raphtory automatically creates documents for each node and edge entity in your graph, optionally you can create documents explicitly as properties and pass the property names to `vectorise()`. This is useful when you already have a deep understanding of your graphs semantics. Additionally, you can cache the embedded graph to disk to avoid having to recompute the vectors when nothing has changed.

## Retrive documents


## Example

