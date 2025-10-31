# Vectorisation

The [vectors][raphtory.vectors] module allows you to transform a graph into a collection of documents and vectorise those documents using an embedding function. Since the AI space moves quickly, Raphtory allows you to plug in your preferred embedding model either locally or from an API.

Using this you can perform [semantic search](https://en.wikipedia.org/wiki/Semantic_search) over your graph data and build powerful AI systems with graph based RAG.

## Vectorise a graph

To vectorise a graph you must create an embeddings function that takes a list of strings and returns a matching list of embeddings. This function can use any model or library you prefer, in this example we use the openai library and direct it to a local API compatible ollama service.

/// tab | :fontawesome-brands-python: Python
```{.python notest}
def get_embeddings(documents, model="embeddinggemma"):
    client = OpenAI(base_url='http://localhost:11434/v1/', api_key="ollama")
    return [client.embeddings.create(input=text, model=model).data[0].embedding for text in documents]

v = g.vectorise(get_embeddings, nodes=node_document, edges=edge_document, verbose=True)
```
///

When you call [Vectorise()][raphtory.GraphView.vectorise] Raphtory automatically creates documents for each node and edge entity in your graph, optionally you can provide template strings to format documents and pass these to `vectorise()`. This is useful when you know which properties are semantically relevant or want to present information in a specific format when retrieved by a human or machine user. Additionally, you can cache the embedded graph to disk to avoid having to recompute the vectors when nothing has changed.

### Document templates

The templates for entity documents follow a subset of [Jinja](https://jinja.palletsprojects.com/en/stable/templates/) and graph attributes and properties are exposed so that you can use them in template expressions.

Most attributes of graph entities are exposed and can be used in Jinja expressions. The nesting of attributes reflects the Python interface and the final result of any chain such as `properties.prop_name` or `src.name` should be a string.

## Retrieve documents

You can retrieve relevant information from the [VectorisedGraph][raphtory.vectors.VectorisedGraph] by making selections.

A [VectorSelection][raphtory.vectors.VectorSelection] is a general object for holding embedded documents, you can create an empty selection or perform a similarity query against a `VectorisedGraph` to populate a new selection.

You can add to a selection by combining existing selections or by adding new documents associated with specific nodes and edges by their IDs. Additionally, you can [expand][raphtory.vectors.VectorSelection.expand_entities_by_similarity] a selection by making similarity queries relative to the entities in the current selection, this uses the power of the graph relationships to constrain your query.

Once you have a selection containing the information you want you can:

- Get the associated graph entities using [nodes()][raphtory.vectors.VectorSelection.nodes] or [edges()][raphtory.vectors.VectorSelection.edges].
- Get the associated documents using [get_documents()][raphtory.vectors.VectorSelection.get_documents] or [get_documents_with_scores()][raphtory.vectors.VectorSelection.get_documents_with_scores].

Each [Document][raphtory.vectors.Document] corresponds to unique entity in the graph, the contents of the associated document and it's vector representation. You can pull any of these out to retrieve information about an entity for a RAG system, compose a subgraph to analyse using Raphtory's algorithms, or feed into some more complex pipeline.

## Asking questions about your network

Using the Network example from the [ingestion using dataframes](../ingestion/3_dataframes.md) discussion you can set up a graph and add some simple AI tools in order to create a `VectorisedGraph`:

/// tab | :fontawesome-brands-python: Python
```{.python notest}
from raphtory import Graph
import pandas as pd
from openai import OpenAI

server_edges_df = pd.read_csv("./network_traffic_edges.csv")
server_edges_df["timestamp"] = pd.to_datetime(server_edges_df["timestamp"])

server_nodes_df = pd.read_csv("./network_traffic_nodes.csv")
server_nodes_df["timestamp"] = pd.to_datetime(server_nodes_df["timestamp"])

traffic_graph = Graph()
traffic_graph.load_edges_from_pandas(
    df=server_edges_df,
    src="source",
    dst="destination",
    time="timestamp",
    properties=["data_size_MB"],
    layer_col="transaction_type",
    metadata=["is_encrypted"],
    shared_metadata={"datasource": "./network_traffic_edges.csv"},
)
traffic_graph.load_nodes_from_pandas(
    df=server_nodes_df,
    id="server_id",
    time="timestamp",
    properties=["OS_version", "primary_function", "uptime_days"],
    metadata=["server_name", "hardware_type"],
    shared_metadata={"datasource": "./network_traffic_edges.csv"},
)

def get_embeddings(documents, model="embeddinggemma"):
    client = OpenAI(base_url='http://localhost:11434/v1/', api_key="ollama")
    return [client.embeddings.create(input=text, model=model).data[0].embedding for text in documents]

def send_query_with_docs(query: str, selection):
    formatted_docs = "\n".join(doc.content for doc in selection.get_documents())
    client = OpenAI(base_url="http://localhost:11434/v1/", api_key="ollama")
    instructions = f"You are helpful assistant. Answer the user question using the following context:\n{formatted_docs}"

    completion = client.chat.completions.create(
        model="gemma3",
        messages = [
        {"role": "system", "content": f"You are helpful assistant. Answer the user question using the following context:\n{formatted_docs}"},
        {"role": "user", "content": query}
        ]
    )
    return completion.choices[0].message.content

v = traffic_graph.vectorise(get_embeddings, verbose=True)
```
///

Using this `VectorisedGraph` you can perform similarity queries and feed the results into an LLM to ground it's responses in your data.

/// tab | :fontawesome-brands-python: Python
```{.python notest}
query = "What's the status of my linux boxes?"

node_selection = v.nodes_by_similarity(query, limit=3)

print(send_query_with_docs(query, node_selection))
```
///

However, you must always be aware that LLM responses are still statistical and variations will occur. In production systems you may want to use a structured output tool to enforce a specific format.

The output of the example query should be similar to the following:

!!! Output
    ```output
    Okay, here’s a rundown of the status of your Linux boxes as of today, September 3, 2023:

    *   **ServerA (Alpha):**
        *   Datasource: ./network_traffic_edges.csv
        *   Hardware Type: Blade Server
        *   OS Version: Ubuntu 20.04 (Changed Sep 1, 2023 08:00)
        *   Primary Function: Database
        *   Uptime: 120 days
    *   **ServerD (Delta):**
        *   Datasource: ./network_traffic_edges.csv
        *   Hardware Type: Tower Server
        *   OS Version: Ubuntu 20.04 (Changed Sep 1, 2023 08:15)
        *   Primary Function: Application Server
        *   Uptime: 60 days
    *   **ServerE (Echo):**
        *   Datasource: ./network_traffic_edges.csv
        *   Hardware Type: Rack Server
        *   OS Version: Red Hat 8.1 (Changed Sep 1, 2023 08:20)
        *   Primary Function: Backup
        *   Uptime: 30 days

    Do you need any more details about any of these servers?
    ```
