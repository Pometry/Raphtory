<br>
<p align="center">
<img src="https://user-images.githubusercontent.com/25484244/218704919-2c725e79-86ee-408d-b1f8-1362d086f876.png" alt="Raphtory"/>
</p>
<br>

<p align="center">
<a href="https://github.com/Raphtory/docbrown/actions/workflows/build.yml/badge.svg">
<img alt="Test and Build" src="https://github.com/Raphtory/docbrown/actions/workflows/build.yml/badge.svg" />
</a>
<a href="https://github.com/Raphtory/docbrown/issues">
<img alt="Issues" src="https://img.shields.io/github/issues/Raphtory/Raphtory?color=brightgreen" />
</a>
</p>
<p align="center">
<a href="https://www.raphtory.com">🌍 Website </a>
&nbsp 
<a href="https://www.pometry.com"><img src="https://user-images.githubusercontent.com/6665739/202438989-2859f8b8-30fb-4402-820a-563049e1fdb3.png" height="20" align="center"/> Pometry</a> 
&nbsp
<a href="https://github.com/Raphtory/docbrown/issues">🐛 Report a Bug</a> 
&nbsp
<a href="https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA"><img src="https://user-images.githubusercontent.com/6665739/154071628-a55fb5f9-6994-4dcf-be03-401afc7d9ee0.png" height="20" align="center"/> Join Slack</a> 
</p>
<br>

# What is Doc Brown? 🥼

Doc Brown is the Rust prototype for the next version of [Raphtory](https://github.com/Raphtory/Raphtory), rethinking several aspects of the underlying graph model and algorithm API. 

Please checkout the [issues](https://github.com/Raphtory/docbrown/issues) for the core features to be included in this version, along with their proposed semantics. 

Below is a diagram of how Doc Brown works:

<p align="center">
<img src="https://user-images.githubusercontent.com/25484244/218711926-944092df-5015-4c7e-8162-34ee044999f4.svg" height=500 alt="Raphtory-DocBrown-Diagram"/>
</p>

GraphDB is the overarching manager for the graph. A GraphDB instance can have N number of shards. These shards (also called TemporalGraphParts) store fragments of a graph. If you have used the Scala/Python version of Raphtory before, the notion of shards is similar to partitions in Raphtory, where parts of a graph are stored into partitions. When an edge or node is added to the graph in Doc Brown, GraphDB will search for an appropriate place inside a shard to place these. In this diagram, there are 4 shards (or partitions) labelled as S1, S2, S3, S4. Altogether they make up the entire temporal graph, hence the name "TemporalGraphPart" for each shard.

Shards are used for performance and distribution reasons. Having multiple shards running in parallel speeds up things a lot in Raphtory. In a matter of seconds, you are able to see your results from your temporal graph analysis. Furthermore, you can run your analysis across multiple machines (e.g. one shard per machine).

# Running Doc Brown 👨🏼‍🔬
The API's are currently in..._Flux_

![image](https://user-images.githubusercontent.com/6665739/214092170-9bf7557c-4b2d-4ec8-baac-911b7ec9fab5.png)

However, here is a quick start guide if you would like to test out the Raphtory Rust prototype.

## Running the LOTR example 🧙🏻‍♂️

### Prerequisites

Make sure you have Rust installed on your OS. Here is a guide to install [Rust](https://doc.rust-lang.org/stable/book/ch01-01-installation.html). 

### 1. Set up Doc Brown 
Clone the Doc Brown Repository and find the examples directory where you can find the Lord of the Rings Example. Create a folder named "Data" under `examples/lotr` and download the LOTR CSV data into this folder. You can download the raw csv [here](https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv).

### 2. Run Doc Brown

Build Doc Brown by running this command to make sure it compiles and builds:
```
cargo build
```
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

### 3. Running Tests

To run tests in Doc Brown, go back into your root folder and run this command:
```
cargo test
```

## Code Example
```rust
// Create your GraphDB object and state the number of shards you would like, here we have 2
let graph = GraphDB::new(2);

// Add vertex and edges to your graph with the respective properties
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

// We calculate a hash for the string "Gandalf" to be used as a unique identifier for Gandalf
let gandalf = utils::calculate_hash(&"Gandalf");

// Get the in-degree, out-degree and degree of Gandalf
let in_degree = graph.degree_window(gandalf, 0, i64::MAX, Direction::IN);
let out_degree = graph.degree_window(gandalf, 0, i64::MAX, Direction::OUT);
let degree = graph.degree_window(gandalf, 0, i64::MAX, Direction::BOTH);
```

# Community  
Join the growing community of open-source enthusiasts using Raphtory to power their graph analysis projects!

- Follow [![Slack](https://img.shields.io/twitter/follow/raphtory?label=@raphtory)](https://twitter.com/raphtory) for the latest Raphtory news and development

- Join our [![Slack](https://img.shields.io/badge/community-Slack-red)](https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA) to chat with us and get answers to your questions!


#### Articles and Talks about Raphtory
- **[Raphtory on the Alan Turing Institute Blog](https://www.turing.ac.uk/blog/just-add-time-dizzying-potential-dynamic-graphs)**
- **[Talk on Raphtory at AI UK 2022](https://www.youtube.com/watch?v=7S9Ymnih-YM&list=PLuD_SqLtxSdVEUsCYlb5XjWm9D6WuNKEz&index=9)**
- **[Talk on Raphtory at KGC 2022](https://www.youtube.com/watch?v=37S4bSN5EaU)**
- **[Talk on Raphtory at NetSciX 2022](https://www.youtube.com/watch?v=QxhrONca4FE)**


# Contributors

<a href="https://github.com/raphtory/docbrown/graphs/contributors"><img src="https://contrib.rocks/image?repo=raphtory/docbrown"/></a>

Since Doc Brown is still a prototype, we are open to any contributions. If you find any issues or would like to work on some issues yourself, visit the [issues](https://github.com/Raphtory/docbrown/issues) page. Join our [Slack](https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA) if you're having any issues or would like to find out more about how you can get stuck in with Raphtory.

# License  

Raphtory is licensed under the terms of the Apache License (check out our LICENSE file).



