# Handling of ambiguous updates

For a *link-stream* graph there is a natural way to construct the graph regardless of the order the updates come in. However, for a *PersistentGraph* where deletions are possible this becomes more difficult. The following examples illustrate this.

## Order of resolving additions and deletions

{{code_block('getting-started/persistent-graph','behaviour_1',['Graph'])}}

Here two edges between Alice and Bob overlap in time: one starting at time 1 and ending at time 5, another starting at time 3 and ending at time 7. 

For *link-stream* graphs in Raphtory are allowed to have edges between the same pair of nodes happening at the same instant. However, when we look at the exploded edges of this *PersistentGraph* graph, the following is returned:

!!! Output

    ```python exec="on" result="text" session="getting-started/persistent-graph"
    --8<-- "python/getting-started/persistent-graph.py:behaviour_1"
    ```

Two edges are created, one that exists from time 1 to time 3 and another that exists from time 3 to time 5. The second deletion at time 7 is ignored. 

The reason for this is that Raphtory's graph updates are inserted in chronological order, so that the same graph is constructed regardless of the order in which the updates are made. With an exception for events which have the same timestamp, which will be covered shortly. 

In this example, the order is: edge addition at time 1, edge addition at time 3, edge deletion at time 5 and edge deletion at time 7. This second edge deletion is now redundant.

## Hanging deletions

Adding edges without a deletion afterwards results in an edge which lasts forever, while deleting an edge without a prior addition does not effect the history. However, hanging deletions are tracked as an object and if the history is later modified to add the corresponding edge at an earlier time the delete will become valid and occur as expected.

{{code_block('getting-started/persistent-graph','hanging_deletions',['Graph'])}}

Which results in the following:
!!! Output

    ```python exec="on" result="text" session="getting-started/persistent-graph"
    --8<-- "python/getting-started/persistent-graph.py:hanging_deletions"
    ```

## Additions and deletions in the same instant

If the update times to an edge are all distinct from each other, the graph that is constructed is fully unambiguous. When events have the same timestamp Raphtory tie-breaks the updates by the order in which they are executed.

In the following, it is impossible to infer what the intended update order is so a tie-break is required.

{{code_block('getting-started/persistent-graph','behaviour_2',['Graph'])}}

!!! Output

    ```python exec="on" result="text" session="getting-started/persistent-graph"
    --8<-- "python/getting-started/persistent-graph.py:behaviour_2"
    ```

This graph has an edge which instantaneously appears and disappears at time 1 and therefore the order of its history is determined by the execution order.

## Interaction with layers

Layering allows different types of interaction to exist, and edges on different layers can have overlapping times in a way that doesn't make sense for edges in the same layer or for edges with no layer. 

Consider an example without layers:

{{code_block('getting-started/persistent-graph','behaviour_1',['Graph'])}}

!!! Output

    ```python exec="on" result="text" session="getting-started/persistent-graph"
    --8<-- "python/getting-started/persistent-graph.py:behaviour_1"
    ```

Now take a look at a  modified example with layers:

{{code_block('getting-started/persistent-graph','behaviour_3',['Graph'])}}

!!! Output

    ```python exec="on" result="text" session="getting-started/persistent-graph"
    --8<-- "python/getting-started/persistent-graph.py:behaviour_3"
    ```

By adding layer names to the different edge instances we produce a different result.

Here we have two edges, one starting and ending at 1 and 5 respectively with the 'colleague' layer, the other starting and ending at 3 and 7 on the 'friends' layer. 
