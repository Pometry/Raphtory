# Raphtory GraphQL docker server example

In this example we run a Raphtory server with the lotr graph embedded in it.

To run the server you just need to type in your shell:

```shell
python build_lotr.py
docker compose up
```

The first command in the shell loads the lotr graph from a csv located in this project at `resource/lotr-without-header.csv` and saves it in binary format in the folder `lotr`. Then, when we start docker compose, we expose that file under the directory `/app/graphs` inside the container, so the server automatically reads it and loads it.

After running it, you should see a message:

```
Playground: http://localhost:1736
```

Now you can follow the link and start making queries to Raphtory. For instance:

```json
{
  graph(name: "lotr") {
    window(tStart: 0, tEnd: 1000) {
      nodes {
        name,
        id
      }
    }
  }
}
```

The output should be:

```json
{
  "data": {
    "graph": {
      "window": {
        "node": {
          "degree": 4
        }
      }
    }
  }
}
```
