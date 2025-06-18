# Querying the graph over time
Raphtory provides six functions: `before()`, `at()`, `after()`, `window()`, `expand()` and `rolling()` for traveling through time and viewing a graph as it was at a specific point, or between two points (applying a time window).

All of these functions can be called on a `graph`, `node`, or `edge`, returning an equivalent `Graph View`, `Node View` or `Edge View` which have all the same functions as its unfiltered counterpart. This means that if you write a function which takes a Raphtory entity, regardless of which filters have been applied.

## Before, At and After

Beginning with the simplest of these filters, `before()`, `at()` and `after()` take a singular `time` argument in epoch (integer) or datetime (string or datetime object) format and return a `View` of the object which includes:

* `before()` - All updates between the beginning of the graph's history and the provided time, exclusive of the time provided.
* `after()` - All updates between the provided time and the end of the graph's history, exclusive of the time provided.
* `at()`- Only updates which happened at exactly the time provided.

While `before()` and `after()` are more useful for continuous time datasets, `at()` can be helpful when you have snapshots or logical timestamps and want to look at them individually or compare/contrast.

In the example below we print the degree of `Lome` across the full dataset, before 12:17 on the 13th of June and after 9:07 on the 30th of June. We also introduce two new time functions here, `start()` and `end()`, which specify a time range for filtering a view.

!!! note
    In this code example we have called the `before()` on the graph and `after()` on the node. This is important, as there are some subtle differences in where these functions are called, which are discussed [below](2_time.md#traversing-the-graph-with-views).

{{code_block('getting-started/querying','at',[])}}
!!! Output

    ```python exec="on" result="text" session="getting-started/querying"
    --8<-- "python/getting-started/querying.py:at"
    ```

## Window
The `window()` function is a more general version of the functions above, allowing you to set both a `start` time as well as an `end` time, inclusive of the start and exclusive of the end time. 

This is useful for digging into specific ranges of the history that you are interested in. In the below example, we look at the number of times `Lome` interacts wth `Nekke` within the full dataset and for one day between the 13th of June and the 14th of June. We use datetime objects in this example, but it would work exactly the same with string dates and epoch integers. 

{{code_block('getting-started/querying','window',[])}}
!!! Output

    ```python exec="on" result="text" session="getting-started/querying"
    --8<-- "python/getting-started/querying.py:window"
    ```

## Traversing the graph with views
There are important differences when applying views depending on which object you call them on because of how filters propagate as you traverse the graph.
 
As a general rule, when you call any function which returns another entity, on a `Graph View`, `Node View` or `Edge View`, the view's filters will be passed onto the entities it returns. For example, if you call `before()` on a graph and then call `node()`, this will return a `Node View` filtered to the time passed to the graph.

However, if this was always the case it would be limiting if you later wanted to explore outside of these bounds.

To allow for both global bounds and moving bounds, if a filter is applied onto the graph, all entities extracted always have this filter applied. However, if a filter is applied to either a `node` or an `edge`, once you have traversed to a new neighbouring `node` this filter is removed. 

As an example of this, below we look at LOME's one hop neighbours before the 20th of June and their neighbours (LOME's two hop neighbours) after the 25th of June. 

First we show calling `before()` on the `graph`. This works for the one hop neighbours, but when `after()` is applied the graph is empty as there is no overlap in dates between the two filters. Next we show calling `before()` on the `node` instead. In this case, once the neighbours have been reached the original filter is removed which allows `after()` to work as desired.

{{code_block('getting-started/querying','hopping',[])}}
!!! Output

    ```python exec="on" result="text" session="getting-started/querying"
    --8<-- "python/getting-started/querying.py:hopping"
    ```

## Expanding
If you have data covering a large period of time, or have many time points of interest, you might use filters and views repeatedly. If there is a pattern to these calls, you can instead use `expanding()`. 

Using `expanding()` will return an iterable of views as if you called `before()` from the earliest time to the latest time at increments of a given `step`. 

The `step` can be specified using a simple epoch integer, or a natural language string describing the interval. For the latter, this is converted into a iterator of datetimes, handling all corner cases like varying month length and leap years.

Within the string you can reference `years`, `months` `weeks`, `days`, `hours`, `minutes`, `seconds` and `milliseconds`. These can be singular or plural and the string can include 'and', spaces, and commas to improve readability.

The example below demonstrates two case. In the first case, we increment through the full history of the graph a week at a time. This creates four views, for each of which we ask how many monkey interactions it has seen. You will notice the start time does not change, but the end time increments by 7 days each view.

The second case shows the complexity of increments Raphtory can handle, stepping by `2 days, 3 hours, 12 minutes and 6 seconds` each time. We have additionally bounded this iterable using a window between the 13th and 23rd of June to demonstrate how these views may be chained.

{{code_block('getting-started/querying','expanding',[])}}
!!! Output

    ```python exec="on" result="text" session="getting-started/querying"
    --8<-- "python/getting-started/querying.py:expanding"
    ```

## Rolling 

You can use `rolling()` to create a rolling window instead of including all prior history. This function will return an iterable of views, incrementing by a `window` size and only including the history from inside the window period, inclusive of start, exclusive of end. This allows you to easily extract daily or monthly metrics.

For example, below we take the code from [expanding](#expanding) and swap out the function for `rolling()`. In the first loop you can see both the start date and end date increase by seven days each time, and the number of monkey interactions sometimes decreases as older data is dropped from the window.

{{code_block('getting-started/querying','rolling_intro',[])}}
!!! Output

    ```python exec="on" result="text" session="getting-started/querying"
    --8<-- "python/getting-started/querying.py:rolling_intro"
    ```

Alongside the window size, `rolling()` takes an optional `step` argument which specifies how far along the timeline it should increment before applying the next window. By default this is the same as `window`, allowing all updates to be analyzed exactly once in non-overlapping windows. 

If you want overlapping or fully disconnected windows, you can set a `step` smaller or greater than the given `window` size. 

As an example of how useful this can be, in the following example we plot the daily unique interactions of `Lome` via `matplotlib` in only 10 lines.

!!! info
    We have to recreate the graph in the first section of this code block so that the output can be rendered as part of the documentation. Please ignore this. 


{{code_block('getting-started/querying','rolling',[])}}