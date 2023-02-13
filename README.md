# What is Doc Brown?

Doc Brown is the Rust prototype for the next version of [Raphtory](https://github.com/Raphtory/Raphtory), rethinking several aspects of the underlying graph model and algorithm API. 

Please checkout the [issues](https://github.com/Raphtory/docbrown/issues) for the core features to be included in this version, along with their proposed semantics. 


# Running Doc Brown
The API's are currently in..._Flux_

![image](https://user-images.githubusercontent.com/6665739/214092170-9bf7557c-4b2d-4ec8-baac-911b7ec9fab5.png)

Here is a quick start guide if you would like to test out the Raphtory Rust prototype.

## Running the Rust LOTR example

Make sure you have Rust installed on your OS. Here is a guide to install [Rust](https://doc.rust-lang.org/stable/book/ch01-01-installation.html).

Clone the Doc Brown Repository and find the examples directory where you can find the Lord of the Rings Example. Create a folder named "Data" under `examples/lotr` and download the LOTR CSV data into this folder. You can download the raw csv [here](https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv).

Run the main function in `main.rs` which creates a graph from the LOTR csv file, showing the different character interactions throughout the book. You should see output that looks something like this with information about the edges and vertices below: 

Loaded graph from encoded data files ./examples/src/bin/lotr/data/graphdb.bincode with 139 vertices, 701 edges which took 0 seconds
Gandalf exists = true

You have run your first Raphtory graph in Rust!

## How to contribute

Since Doc Brown is still a prototype, we are open to any contributions. If you find any issues or would like to work on some issues yourself, visit the [issues](https://github.com/Raphtory/docbrown/issues) page. Join our [Slack](https://join.slack.com/t/raphtory/shared_invite/zt-xbebws9j-VgPIFRleJFJBwmpf81tvxA) if you're having any issues or would like to find out more about how you can get stuck in with Raphtory.


