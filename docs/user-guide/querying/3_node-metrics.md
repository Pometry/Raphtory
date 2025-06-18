
# Node metrics and functions
Nodes can be accessed by storing the object returned from a call to `add_node()`, by directly asking for a specific entity via `node()`, or by iterating over all entities via `nodes`. Once you have a node, you can ask it some questions. 

## Update history 

Nodes have functions for querying their earliest and latest update time (as an epoch or datetime) as well as for accessing their full history (using `history()` or `history_date_time()`). In the code below we create a node object for the monkey `Felipe` and see when their updates occurred. 

```python
v = g.node("FELIPE")
print(
    f"{v.name}'s first interaction was at {v.earliest_date_time} and their last interaction was at {v.latest_date_time}\n"
)
history = v.history_date_time()
# We format the returned datetime objects here to make the list more readable
history_formatted = [date.strftime("%Y-%m-%d %H:%M:%S") for date in history]

print(f"{v.name} had interactions at the following times: {history_formatted}\n")
```


!!! Output

    ```python
    FELIPE's first interaction was at 2019-06-13 09:50:00+00:00 and their last interaction was at 2019-07-10 11:05:00+00:00

    FELIPE had interactions at the following times: ['2019-06-13 09:50:00', '2019-06-13 09:51:00', '2019-06-13 09:52:00', '2019-06-13 09:53:00', '2019-06-13 09:54:00', '2019-06-13 10:12:00', '2019-06-13 10:21:00', '2019-06-13 10:43:00', '2019-06-13 10:56:00', '2019-06-13 10:57:00', '2019-06-13 11:00:00', '2019-06-13 14:50:00', '2019-06-13 14:51:00', '2019-06-13 14:54:00', '2019-06-13 15:32:00', '2019-06-13 15:33:00', '2019-06-13 15:36:00', '2019-06-13 15:44:00', '2019-06-13 16:03:00', '2019-06-13 16:04:00', '2019-06-13 16:06:00', '2019-06-13 16:24:00', '2019-06-14 09:20:00', '2019-06-14 09:29:00', '2019-06-14 10:06:00', '2019-06-14 10:19:00', '2019-06-14 10:32:00', '2019-06-14 10:33:00', '2019-06-14 10:35:00', '2019-06-14 14:41:00', '2019-06-14 14:47:00', '2019-06-14 14:48:00', '2019-06-14 14:49:00', '2019-06-14 14:51:00', '2019-06-14 15:08:00', '2019-06-14 15:20:00', '2019-06-14 15:22:00', '2019-06-14 15:29:00', '2019-06-14 15:31:00', '2019-06-14 15:32:00', '2019-06-17 11:02:00', '2019-06-17 11:04:00', '2019-06-17 12:49:00', '2019-06-17 14:01:00', '2019-06-17 14:02:00', '2019-06-18 10:25:00', '2019-06-18 10:36:00', '2019-06-18 10:40:00', '2019-06-18 11:01:00', '2019-06-18 13:42:00', '2019-06-18 13:44:00', '2019-06-18 13:45:00', '2019-06-19 08:56:00', '2019-06-19 08:58:00', '2019-06-19 08:59:00', '2019-06-19 09:11:00', '2019-06-19 09:12:00', '2019-06-19 09:14:00', '2019-06-19 09:26:00', '2019-06-19 09:31:00', '2019-06-19 09:42:00', '2019-06-19 10:25:00', '2019-06-19 10:26:00', '2019-06-19 10:27:00', '2019-06-19 11:25:00', '2019-06-19 12:01:00', '2019-06-19 12:05:00', '2019-06-19 12:06:00', '2019-06-19 12:23:00', '2019-06-19 12:34:00', '2019-06-19 12:43:00', '2019-06-19 12:48:00', '2019-06-19 12:49:00', '2019-06-19 12:50:00', '2019-06-20 10:05:00', '2019-06-20 10:15:00', '2019-06-20 10:36:00', '2019-06-20 10:51:00', '2019-06-20 11:17:00', '2019-06-20 14:56:00', '2019-06-20 14:57:00', '2019-06-20 14:59:00', '2019-06-20 15:08:00', '2019-06-20 15:09:00', '2019-06-20 15:20:00', '2019-06-20 15:27:00', '2019-06-20 15:30:00', '2019-06-20 15:55:00', '2019-06-21 09:43:00', '2019-06-21 10:38:00', '2019-06-21 10:39:00', '2019-06-21 11:11:00', '2019-06-21 11:14:00', '2019-06-21 11:39:00', '2019-06-21 11:40:00', '2019-06-21 11:46:00', '2019-06-21 11:47:00', '2019-06-21 11:48:00', '2019-06-21 11:49:00', '2019-06-21 12:03:00', '2019-06-21 12:41:00', '2019-06-21 12:57:00', '2019-06-21 13:00:00', '2019-06-21 13:01:00', '2019-06-21 13:02:00', '2019-06-24 10:54:00', '2019-06-24 10:56:00', '2019-06-24 10:57:00', '2019-06-24 10:58:00', '2019-06-24 10:59:00', '2019-06-24 11:01:00', '2019-06-24 11:08:00', '2019-06-24 11:09:00', '2019-06-24 11:14:00', '2019-06-24 11:22:00', '2019-06-24 15:40:00', '2019-06-24 15:41:00', '2019-06-24 15:42:00', '2019-06-24 15:43:00', '2019-06-24 16:09:00', '2019-06-24 16:11:00', '2019-06-25 10:25:00', '2019-06-25 10:30:00', '2019-06-25 10:48:00', '2019-06-25 10:49:00', '2019-06-25 11:13:00', '2019-06-25 11:14:00', '2019-06-25 11:26:00', '2019-06-25 15:11:00', '2019-06-25 15:17:00', '2019-06-25 15:52:00', '2019-06-25 15:54:00', '2019-06-25 16:03:00', '2019-06-25 16:04:00', '2019-06-25 16:09:00', '2019-06-26 09:10:00', '2019-06-26 09:11:00', '2019-06-26 09:32:00', '2019-06-26 09:33:00', '2019-06-26 09:53:00', '2019-06-26 09:54:00', '2019-06-26 09:58:00', '2019-06-26 10:05:00', '2019-06-26 10:06:00', '2019-06-26 10:37:00', '2019-06-26 10:39:00', '2019-06-26 13:22:00', '2019-06-26 13:35:00', '2019-06-26 13:39:00', '2019-06-26 13:40:00', '2019-06-26 14:17:00', '2019-06-26 14:18:00', '2019-06-27 09:31:00', '2019-06-27 09:37:00', '2019-06-27 12:29:00', '2019-06-27 12:46:00', '2019-06-27 13:03:00', '2019-06-27 13:49:00', '2019-06-27 13:52:00', '2019-06-27 13:53:00', '2019-06-28 10:15:00', '2019-06-28 10:16:00', '2019-06-28 10:17:00', '2019-06-28 10:18:00', '2019-06-28 10:19:00', '2019-06-28 10:20:00', '2019-06-28 11:12:00', '2019-06-28 11:13:00', '2019-07-01 08:44:00', '2019-07-01 08:46:00', '2019-07-01 08:49:00', '2019-07-01 08:51:00', '2019-07-01 09:06:00', '2019-07-01 09:21:00', '2019-07-01 10:13:00', '2019-07-01 13:20:00', '2019-07-01 14:45:00', '2019-07-02 08:52:00', '2019-07-02 13:32:00', '2019-07-02 13:33:00', '2019-07-02 13:34:00', '2019-07-02 13:35:00', '2019-07-02 13:36:00', '2019-07-02 14:35:00', '2019-07-02 14:39:00', '2019-07-03 09:27:00', '2019-07-03 09:39:00', '2019-07-03 09:41:00', '2019-07-03 10:14:00', '2019-07-03 10:15:00', '2019-07-03 10:16:00', '2019-07-03 10:17:00', '2019-07-03 10:18:00', '2019-07-03 10:54:00', '2019-07-03 11:16:00', '2019-07-03 11:17:00', '2019-07-03 11:37:00', '2019-07-03 12:03:00', '2019-07-03 12:05:00', '2019-07-04 09:30:00', '2019-07-04 09:31:00', '2019-07-04 09:39:00', '2019-07-04 09:45:00', '2019-07-04 09:46:00', '2019-07-04 10:00:00', '2019-07-04 10:25:00', '2019-07-04 10:55:00', '2019-07-04 10:59:00', '2019-07-04 14:02:00', '2019-07-04 14:04:00', '2019-07-04 14:05:00', '2019-07-04 14:30:00', '2019-07-04 14:38:00', '2019-07-04 15:09:00', '2019-07-04 15:19:00', '2019-07-05 10:10:00', '2019-07-05 10:12:00', '2019-07-05 10:13:00', '2019-07-05 10:20:00', '2019-07-05 10:46:00', '2019-07-05 11:05:00', '2019-07-05 11:40:00', '2019-07-05 11:41:00', '2019-07-05 11:57:00', '2019-07-05 12:57:00', '2019-07-05 13:02:00', '2019-07-05 13:07:00', '2019-07-05 13:13:00', '2019-07-08 11:34:00', '2019-07-08 11:39:00', '2019-07-08 11:40:00', '2019-07-08 11:46:00', '2019-07-08 11:50:00', '2019-07-08 12:37:00', '2019-07-08 14:32:00', '2019-07-08 14:33:00', '2019-07-08 15:42:00', '2019-07-08 15:48:00', '2019-07-08 15:51:00', '2019-07-09 10:57:00', '2019-07-09 10:59:00', '2019-07-09 11:01:00', '2019-07-09 11:17:00', '2019-07-09 11:20:00', '2019-07-09 13:37:00', '2019-07-09 13:38:00', '2019-07-09 13:39:00', '2019-07-09 13:40:00', '2019-07-09 14:34:00', '2019-07-09 14:36:00', '2019-07-09 14:37:00', '2019-07-10 10:00:00', '2019-07-10 10:06:00', '2019-07-10 10:14:00', '2019-07-10 10:30:00', '2019-07-10 10:44:00', '2019-07-10 11:05:00']
    ```

## Neighbours, edges and paths
To investigate who a node is connected with we can ask for its `degree()`, `edges`, or `neighbours`. As Raphtory graphs are directed, all of these functions also have an `in_` and `out_` variation, allowing you get only incoming and outgoing connections respectively. These functions return the following:

* **degree:** A count of the number of unique connections a node has
* **edges:** An `Edges` iterable of edge objects, one for each unique `(src,dst)` pair
* **neighbours:** A `PathFromNode` iterable of node objects, one for each entity the original node shares an edge with

In the code below we call a selection of these functions to show the sort of questions you may ask. 

!!! info

    The final section of the code makes use of `v.neighbours.name.collect()` - this is a chain of functions which are run on each node in the `PathFromNode` iterable. We will discuss these sort of operations more in [Chaining functions](../querying/6_chaining.md). 

```python
v = g.node("FELIPE")
v_name = v.name
in_degree = v.in_degree()
out_degree = v.out_degree()
in_edges = v.in_edges
neighbours = v.neighbours
neighbour_names = v.neighbours.name.collect()

print(
    f"{v_name} has {in_degree} incoming interactions and {out_degree} outgoing interactions.\n"
)
print(in_edges)
print(neighbours, "\n")
print(f"{v_name} interacted with the following baboons {neighbour_names}")
```

!!! Output

    ```python
    FELIPE has 17 incoming interactions and 18 outgoing interactions.

    Edges(Edge(source=MALI, target=FELIPE, earliest_time=1561117140000, latest_time=1562753160000, properties={Weight: 1}, layer(s)=[Grooming, Resting, Presenting]), Edge(source=LOME, target=FELIPE, earliest_time=1560421260000, latest_time=1562149080000, properties={Weight: 1}, layer(s)=[Grooming, Resting, Playing with, Presenting, Chasing]), Edge(source=NEKKE, target=FELIPE, earliest_time=1560443040000, latest_time=1562596380000, properties={Weight: 1}, layer(s)=[Touching, Resting, Presenting, Grunting-Lipsmacking, Embracing]), Edge(source=PETOULETTE, target=FELIPE, earliest_time=1561628220000, latest_time=1562252940000, properties={Weight: 1}, layer(s)=[Resting]), Edge(source=EWINE, target=FELIPE, earliest_time=1560523260000, latest_time=1562585640000, properties={Weight: 1}, layer(s)=[Grooming, Resting, Presenting, Avoiding]), Edge(source=ANGELE, target=FELIPE, earliest_time=1560419400000, latest_time=1562753640000, properties={Weight: 1}, layer(s)=[Grooming, Resting, Presenting, Grunting-Lipsmacking, Submission, Copulating]), Edge(source=VIOLETTE, target=FELIPE, earliest_time=1560439920000, latest_time=1561373760000, properties={Weight: 1}, layer(s)=[Grooming, Resting, Presenting]), Edge(source=BOBO, target=FELIPE, earliest_time=1560423360000, latest_time=1561543080000, properties={Weight: -1}, layer(s)=[Presenting, Grunting-Lipsmacking, Avoiding]), Edge(source=MAKO, target=FELIPE, earliest_time=1560937320000, latest_time=1562679600000, properties={Weight: 1}, layer(s)=[Grooming, Resting, Playing with, Presenting, Grunting-Lipsmacking, Embracing]), Edge(source=FEYA, target=FELIPE, earliest_time=1560853500000, latest_time=1562586000000, properties={Weight: 1}, layer(s)=[Resting, Presenting]), ...)
    PathFromNode(Node(name=MALI, earliest_time=1560422040000, latest_time=1562755320000), Node(name=LOME, earliest_time=1560419520000, latest_time=1562756100000), Node(name=NEKKE, earliest_time=1560419520000, latest_time=1562756700000), Node(name=PETOULETTE, earliest_time=1560422520000, latest_time=1562754420000), Node(name=EWINE, earliest_time=1560442020000, latest_time=1562754600000), Node(name=ANGELE, earliest_time=1560419400000, latest_time=1562754600000), Node(name=VIOLETTE, earliest_time=1560423600000, latest_time=1562754900000), Node(name=BOBO, earliest_time=1560419520000, latest_time=1562755500000), Node(name=MAKO, earliest_time=1560421620000, latest_time=1562756100000), Node(name=FEYA, earliest_time=1560420000000, latest_time=1562756040000), ...) 
    
    FELIPE interacted with the following baboons ['MALI', 'LOME', 'NEKKE', 'PETOULETTE', 'EWINE', 'ANGELE', 'VIOLETTE', 'BOBO', 'MAKO', 'FEYA', 'LIPS', 'ATMOSPHERE', 'FANA', 'MUSE', 'HARLEM', 'PIPO', 'ARIELLE', 'SELF']
    ```