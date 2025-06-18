
# Chaining functions

The Raphtory iterables `Nodes`,`Edges`, and `Properties` are [lazy](https://en.wikipedia.org/wiki/Lazy_evaluation) data structures which allow you to chain multiple functions together before a final execution. 

For a node `v`, using the chain `v.neighbours.neighbours` will return the two-hop neighbours. The first call of `neighbours` returns the immediate neighbours of `v`, the second applies the`neighbours` function to each of the nodes returned by the first call. 

You can continue this chain indefinitely with any functions in the `Node`, `Edge` or `Property` API until either: 

* Calling `.collect()`, which will execute the chain and return the result.
* Executing the chain by handing it to a python function such as `list()`, `set()`, `sum()`, etc.
* Iterating through the chain via a loop/list comprehension.

The following example gets the names of all the monkeys, the names of their two-hop neighbours, and zips these together before printing the result.

{{code_block('getting-started/querying','function_chains',['Edges'])}}
!!! Output

    ```python exec="on" result="text" session="getting-started/querying"
    --8<-- "python/getting-started/querying.py:function_chains"
    ```

## Chains with properties
To demonstrate some more complex questions that you could ask using Raphtory, the following example includes some property aggregation into our chains. 

First we sum the `Weight` value of each of `Felipe's` out-neighbours to rank them by the number of positive interactions he has initiated with them. Following this we can find the most annoying monkey by ranking globally who, on average, has had the most negative interactions initiated against them.

{{code_block('getting-started/querying','friendship',['Edges'])}}
!!! Output

    ```python exec="on" result="text" session="getting-started/querying"
    --8<-- "python/getting-started/querying.py:friendship"
    ```