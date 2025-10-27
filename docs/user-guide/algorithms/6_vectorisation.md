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

For example, in a money laundering case a simple template might be:

/// tab | :fontawesome-brands-python: Python
```{.python notest}
node_document = """
{% if properties.type == "Company" %}
{{ name }} is a company with the following details:
Employee count: {{ properties.employeeCount}}
Account: {{ properties.account}}
Location: {{ properties.location}}
Jurisdiction: {{ properties.jurisdiction}}
Partnerships: {{ properties.partnerships}}
{% endif %}

{% if properties.type == "Person" %}
{{ name }} is a director with the follwing details:
Age: {{ properties.age }}
Mobile: {{ properties.mobile }}
Home address: {{ properties.homeAddress }}
Email: {{ properties.email }}
{% endif %}

{% if properties.type == "Report" %}
{{name}} is a suspicious activity report with the following content:
{{ properties.document }}
{% endif %}
"""

edge_document = """
{% if layers[0] == "report" %}
{{ src.name }} was raised against {{ dst.name}}
{% elif layers[0] == "director" %}
{{ dst.name }} is a director of {{ src.name }}
{% else %}
{{ src.name }} transferred ${{ properties.amount_usd }} to {{ dst.name }}
{% endif %}
"""
```
///

## Retrieve documents

You can retrieve relevant information from the [VectorisedGraph][raphtory.vectors.VectorisedGraph] by making selections.

A [VectorSelection][raphtory.vectors.VectorSelection] is a general object for holding embedded documents, you can create an empty selection or perform a similarity query against a `VectorisedGraph` to populate a new selection.

You can add to a selection by combining existing selections or by adding new documents associated with specific nodes and edges by their IDs. Additionally, you can [expand][raphtory.vectors.VectorSelection.expand_entities_by_similarity] a selection by making similarity queries relative to the entities in the current selection, this uses the power of the graph relationships to constrain your query.

Once you have a selection containing the information you want you can:

- Get the associated graph entities using [nodes()][raphtory.vectors.VectorSelection.nodes] or [edges()][raphtory.vectors.VectorSelection.edges].
- Get the associated documents using [get_documents()][raphtory.vectors.VectorSelection.get_documents] or [get_documents_with_scores()][raphtory.vectors.VectorSelection.get_documents_with_scores].

Each [Document][raphtory.vectors.Document] corresponds to unique entity in the graph, the contents of the associated document and it's vector representation. You can pull any of these out to retrieve information about an entity for a RAG system, compose a subgraph to analyse using Raphtory's algorithms, or feed into some more complex pipeline.

## Example

