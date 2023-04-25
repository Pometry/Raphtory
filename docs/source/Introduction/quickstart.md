# Quickstart Example

## Prerequisites

Please ensure you have installed and set-up raphtory. 

### 1. Download the data 

Create a folder named "Data" under `examples/lotr` and download the LOTR CSV data into this folder. You can download the raw csv [here](https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv).


### 2. Run the LOTR Example

Next run the main function in `main.rs` which creates a graph from the LOTR csv file, showing the different character interactions throughout the book. To do this, you will need to be in the `lotr` folder, the file path to this from root is `./examples/src/bin/lotr`. Once you are here run this command to run the LOTR example:

```
cargo run --bin lotr
```

You should see output that looks something like this with information about the edges and vertices below: 

```
Loaded graph from encoded data files ./examples/src/bin/lotr/data/graphdb.bincode with 139 vertices, 701 edges which took 0 seconds

Gandalf exists = true
```

Congratulations, you have run your first Raphtory graph in Rust!


## Code Example

### How to create a graph

#### 1. Create your GraphDB object and state the number of shards you would like, here we have 2

```rust
let graph = GraphDB::new(2);
```

#### 2. Add edges and vertices with their properties 

```rust
graph.add_vertex(
  src_id,
  time,
  &vec![("name".to_string(), Prop::Str("Character".to_string()))],
);

graph.add_vertex(
  dst_id,
  time,
  &vec![("name".to_string(), Prop::Str("Character".to_string()))],
);

graph.add_edge(
  src_id,
  dst_id,
  time,
  &vec![(
      "name".to_string(),
      Prop::Str("Character Co-occurrence".to_string()),
  )],
);
````

#### 3. Run a few algorithms 

Get the in-degree, out-degree and degree of a particular vertex. 

We calculate a hash for the string "Gandalf" to be used as a unique identifier for Gandalf. 

```rust

let gandalf = utils::calculate_hash(&"Gandalf");

let in_degree = graph.degree_window(gandalf, 0, i64::MAX, Direction::IN);
let out_degree = graph.degree_window(gandalf, 0, i64::MAX, Direction::OUT);
let degree = graph.degree_window(gandalf, 0, i64::MAX, Direction::BOTH);
```


