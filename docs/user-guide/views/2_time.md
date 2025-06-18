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

=== ":fontawesome-brands-python: Python"

    ```python
    v = g.node("LOME")

    print(f"Across the full dataset {v.name} interacted with {v.degree()} other monkeys.")

    v_before = g.before(1560428239000).node("LOME")  # 13/06/2019 12:17:19 as epoch
    print(
        f"Between {v_before.start_date_time} and {v_before.end_date_time}, {v_before.name} interacted with {v_before.degree()} other monkeys."
    )

    v_after = g.node("LOME").after("2019-06-30 9:07:31")
    print(
        f"Between {v_after.start_date_time} and {v_after.end_date_time}, {v_after.name} interacted with {v_after.degree()} other monkeys."
    )
    ```

!!! Output

    ```output
    Across the full dataset LOME interacted with 18 other monkeys.
    Between None and 2019-06-13 12:17:19+00:00, LOME interacted with 5 other monkeys.
    Between 2019-06-30 09:07:31.001000+00:00 and None, LOME interacted with 17 other monkeys.
    ```

## Window
The `window()` function is a more general version of the functions above, allowing you to set both a `start` time as well as an `end` time, inclusive of the start and exclusive of the end time. 

This is useful for digging into specific ranges of the history that you are interested in. In the below example, we look at the number of times `Lome` interacts wth `Nekke` within the full dataset and for one day between the 13th of June and the 14th of June. We use datetime objects in this example, but it would work exactly the same with string dates and epoch integers. 

=== ":fontawesome-brands-python: Python"

    ```python
    from datetime import datetime

    start_day = datetime.strptime("2019-06-13", "%Y-%m-%d")
    end_day = datetime.strptime("2019-06-14", "%Y-%m-%d")
    e = g.edge("LOME", "NEKKE")
    print(
        f"Across the full dataset {e.src.name} interacted with {e.dst.name} {len(e.history())} times"
    )
    e = e.window(start_day, end_day)
    print(
        f"Between {e.start_date_time} and {e.end_date_time}, {e.src.name} interacted with {e.dst.name} {len(e.history())} times"
    )
    print(
        f"Window start: {e.start_date_time}, First update: {e.earliest_date_time}, Last update: {e.latest_date_time}, Window End: {e.end_date_time}"
    )
    ```

!!! Output

    ```output
    Across the full dataset LOME interacted with NEKKE 41 times
    Between 2019-06-13 00:00:00+00:00 and 2019-06-14 00:00:00+00:00, LOME interacted with NEKKE 8 times
    Window start: 2019-06-13 00:00:00+00:00, First update: 2019-06-13 10:18:00+00:00, Last update: 2019-06-13 15:05:00+00:00, Window End: 2019-06-14 00:00:00+00:00
    ```

## Traversing the graph with views
There are important differences when applying views depending on which object you call them on because of how filters propagate as you traverse the graph.
 
As a general rule, when you call any function which returns another entity, on a `Graph View`, `Node View` or `Edge View`, the view's filters will be passed onto the entities it returns. For example, if you call `before()` on a graph and then call `node()`, this will return a `Node View` filtered to the time passed to the graph.

However, if this was always the case it would be limiting if you later wanted to explore outside of these bounds.

To allow for both global bounds and moving bounds, if a filter is applied onto the graph, all entities extracted always have this filter applied. However, if a filter is applied to either a `node` or an `edge`, once you have traversed to a new neighbouring `node` this filter is removed. 

As an example of this, below we look at LOME's one hop neighbours before the 20th of June and their neighbours (LOME's two hop neighbours) after the 25th of June. 

First we show calling `before()` on the `graph`. This works for the one hop neighbours, but when `after()` is applied the graph is empty as there is no overlap in dates between the two filters. Next we show calling `before()` on the `node` instead. In this case, once the neighbours have been reached the original filter is removed which allows `after()` to work as desired.

=== ":fontawesome-brands-python: Python"

    ```python
    first_day = datetime.strptime("2019-06-20", "%Y-%m-%d")
    second_day = datetime.strptime("2019-06-25", "%Y-%m-%d")

    one_hop_neighbours = g.before(first_day).node("LOME").neighbours.name.collect()
    two_hop_neighbours = (
        g.before(first_day).node("LOME").neighbours.after(second_day).neighbours.collect()
    )
    print(
        f"When the before is applied to the graph, LOME's one hop neighbours are: {one_hop_neighbours}"
    )
    print(
        f"When the before is applied to the graph, LOME's two hop neighbours are: {two_hop_neighbours}"
    )
    one_hop_neighbours = g.node("LOME").before(first_day).neighbours.name.collect()
    two_hop_neighbours = (
        g.node("LOME")
        .before(first_day)
        .neighbours.after(second_day)
        .neighbours.name.collect()
    )
    print(
        f"When the before is applied to the node, LOME's one hop neighbours are: {one_hop_neighbours}"
    )
    print(
        f"When the before is applied to the node, LOME's two hop neighbours are: {two_hop_neighbours}"
    )
    ```

!!! Output

    ```output
    When the before is applied to the graph, LOME's one hop neighbours are: ['MALI', 'NEKKE', 'EWINE', 'ANGELE', 'VIOLETTE', 'BOBO', 'MAKO', 'FEYA', 'FELIPE', 'LIPS', 'ATMOSPHERE', 'FANA', 'MUSE', 'HARLEM']
    When the before is applied to the graph, LOME's two hop neighbours are: []
    When the before is applied to the node, LOME's one hop neighbours are: ['MALI', 'NEKKE', 'EWINE', 'ANGELE', 'VIOLETTE', 'BOBO', 'MAKO', 'FEYA', 'FELIPE', 'LIPS', 'ATMOSPHERE', 'FANA', 'MUSE', 'HARLEM']
    When the before is applied to the node, LOME's two hop neighbours are: ['MALI', 'LOME', 'NEKKE', 'PETOULETTE', 'EWINE', 'ANGELE', 'VIOLETTE', 'BOBO', 'MAKO', 'FEYA', 'FELIPE', 'LIPS', 'FANA', 'MUSE', 'HARLEM', 'ARIELLE', 'MALI', 'LOME', 'PETOULETTE', 'EWINE', 'ANGELE', 'VIOLETTE', 'BOBO', 'MAKO', 'FEYA', 'FELIPE', 'LIPS', 'ATMOSPHERE', 'FANA', 'MUSE', 'HARLEM', 'ARIELLE', 'MALI', 'LOME', 'NEKKE', 'PETOULETTE', 'ANGELE', 'VIOLETTE', 'BOBO', 'MAKO', 'FELIPE', 'LIPS', 'ATMOSPHERE', 'FANA', 'MUSE', 'HARLEM', 'KALI', 'ARIELLE', 'MALI', 'LOME', 'NEKKE', 'PETOULETTE', 'EWINE', 'VIOLETTE', 'MAKO', 'FEYA', 'FELIPE', 'LIPS', 'ATMOSPHERE', 'FANA', 'MUSE', 'KALI', 'ARIELLE', 'MALI  ', 'MALI', 'LOME', 'NEKKE', 'PETOULETTE', 'EWINE', 'ANGELE', 'BOBO', 'MAKO', 'FEYA', 'ATMOSPHERE', 'MUSE', 'HARLEM', 'PIPO', 'KALI', 'ARIELLE', 'EXTERNE', 'MALI', 'LOME', 'NEKKE', 'PETOULETTE', 'EWINE', 'VIOLETTE', 'MAKO', 'FEYA', 'FELIPE', 'LIPS', 'ATMOSPHERE', 'FANA', 'MUSE', 'HARLEM', 'PIPO', 'KALI', 'ARIELLE', 'MALI', 'LOME', 'NEKKE', 'PETOULETTE', 'EWINE', 'ANGELE', 'VIOLETTE', 'BOBO', 'MAKO', 'FEYA', 'FELIPE', 'LIPS', 'ATMOSPHERE', 'FANA', 'MUSE', 'HARLEM', 'KALI', 'MALI', 'LOME', 'NEKKE', 'PETOULETTE', 'ANGELE', 'VIOLETTE', 'BOBO', 'MAKO', 'FELIPE', 'LIPS', 'ATMOSPHERE', 'FANA', 'MUSE', 'HARLEM', 'ARIELLE', 'MALI', 'LOME', 'NEKKE', 'PETOULETTE', 'EWINE', 'ANGELE', 'BOBO', 'MAKO', 'FEYA', 'LIPS', 'ATMOSPHERE', 'FANA', 'MUSE', 'HARLEM', 'PIPO', 'ARIELLE', 'MALI', 'LOME', 'NEKKE', 'PETOULETTE', 'EWINE', 'ANGELE', 'BOBO', 'MAKO', 'FEYA', 'FELIPE', 'FANA', 'MUSE', 'HARLEM', 'PIPO', 'KALI', 'ARIELLE', 'LOME', 'NEKKE', 'EWINE', 'ANGELE', 'VIOLETTE', 'BOBO', 'MAKO', 'FEYA', 'FELIPE', 'FANA', 'MUSE', 'HARLEM', 'PIPO', 'KALI', 'ARIELLE', 'MALI', 'LOME', 'NEKKE', 'PETOULETTE', 'EWINE', 'ANGELE', 'BOBO', 'MAKO', 'FEYA', 'FELIPE', 'LIPS', 'ATMOSPHERE', 'MUSE', 'HARLEM', 'PIPO', 'KALI', 'ARIELLE', 'MALI', 'LOME', 'NEKKE', 'PETOULETTE', 'EWINE', 'ANGELE', 'VIOLETTE', 'BOBO', 'MAKO', 'FEYA', 'FELIPE', 'LIPS', 'ATMOSPHERE', 'FANA', 'HARLEM', 'PIPO', 'ARIELLE', 'MALI', 'LOME', 'NEKKE', 'EWINE', 'VIOLETTE', 'BOBO', 'MAKO', 'FEYA', 'FELIPE', 'LIPS', 'ATMOSPHERE', 'FANA', 'MUSE', 'PIPO', 'ARIELLE']
    ```

## Expanding
If you have data covering a large period of time, or have many time points of interest, you might use filters and views repeatedly. If there is a pattern to these calls, you can instead use `expanding()`. 

Using `expanding()` will return an iterable of views as if you called `before()` from the earliest time to the latest time at increments of a given `step`. 

The `step` can be specified using a simple epoch integer, or a natural language string describing the interval. For the latter, this is converted into a iterator of datetimes, handling all corner cases like varying month length and leap years.

Within the string you can reference `years`, `months` `weeks`, `days`, `hours`, `minutes`, `seconds` and `milliseconds`. These can be singular or plural and the string can include 'and', spaces, and commas to improve readability.

The example below demonstrates two case. In the first case, we increment through the full history of the graph a week at a time. This creates four views, for each of which we ask how many monkey interactions it has seen. You will notice the start time does not change, but the end time increments by 7 days each view.

The second case shows the complexity of increments Raphtory can handle, stepping by `2 days, 3 hours, 12 minutes and 6 seconds` each time. We have additionally bounded this iterable using a window between the 13th and 23rd of June to demonstrate how these views may be chained.

=== ":fontawesome-brands-python: Python"

    ```python
    print(
        f"The full range of time in the graph is {g.earliest_date_time} to {g.latest_date_time}\n"
    )

    for expanding_g in g.expanding("1 week"):
        print(
            f"From {expanding_g.start_date_time} to {expanding_g.end_date_time} there were {expanding_g.count_temporal_edges()} monkey interactions"
        )

    print()
    start_day = datetime.strptime("2019-06-13", "%Y-%m-%d")
    end_day = datetime.strptime("2019-06-23", "%Y-%m-%d")
    for expanding_g in g.window(start_day, end_day).expanding(
        "2 days, 3 hours, 12 minutes and 6 seconds"
    ):
        print(
            f"From {expanding_g.start_date_time} to {expanding_g.end_date_time} there were {expanding_g.count_temporal_edges()} monkey interactions"
        )
    ```

!!! Output

    ```output
    The full range of time in the graph is 2019-06-13 09:50:00+00:00 to 2019-07-10 11:05:00+00:00

    From None to 2019-06-20 09:50:00+00:00 there were 789 monkey interactions
    From None to 2019-06-27 09:50:00+00:00 there were 1724 monkey interactions
    From None to 2019-07-04 09:50:00+00:00 there were 2358 monkey interactions
    From None to 2019-07-11 09:50:00+00:00 there were 3196 monkey interactions

    From 2019-06-13 00:00:00+00:00 to 2019-06-15 03:12:06+00:00 there were 377 monkey interactions
    From 2019-06-13 00:00:00+00:00 to 2019-06-17 06:24:12+00:00 there were 377 monkey interactions
    From 2019-06-13 00:00:00+00:00 to 2019-06-19 09:36:18+00:00 there were 691 monkey interactions
    From 2019-06-13 00:00:00+00:00 to 2019-06-21 12:48:24+00:00 there were 1143 monkey interactions
    From 2019-06-13 00:00:00+00:00 to 2019-06-23 00:00:00+00:00 there were 1164 monkey interactions
    ```

## Rolling 

You can use `rolling()` to create a rolling window instead of including all prior history. This function will return an iterable of views, incrementing by a `window` size and only including the history from inside the window period, inclusive of start, exclusive of end. This allows you to easily extract daily or monthly metrics.

For example, below we take the code from [expanding](#expanding) and swap out the function for `rolling()`. In the first loop you can see both the start date and end date increase by seven days each time, and the number of monkey interactions sometimes decreases as older data is dropped from the window.

=== ":fontawesome-brands-python: Python"

    ```python
    print("Rolling 1 week")
    for rolling_g in g.rolling(window="1 week"):
        print(
            f"From {rolling_g.start_date_time} to {rolling_g.end_date_time} there were {rolling_g.count_temporal_edges()} monkey interactions"
        )
    ```

!!! Output

    ```output
    Rolling 1 week
    From 2019-06-13 09:50:00+00:00 to 2019-06-20 09:50:00+00:00 there were 789 monkey interactions
    From 2019-06-20 09:50:00+00:00 to 2019-06-27 09:50:00+00:00 there were 935 monkey interactions
    From 2019-06-27 09:50:00+00:00 to 2019-07-04 09:50:00+00:00 there were 634 monkey interactions
    From 2019-07-04 09:50:00+00:00 to 2019-07-11 09:50:00+00:00 there were 838 monkey interactions
    ```

Alongside the window size, `rolling()` takes an optional `step` argument which specifies how far along the timeline it should increment before applying the next window. By default this is the same as `window`, allowing all updates to be analyzed exactly once in non-overlapping windows. 

If you want overlapping or fully disconnected windows, you can set a `step` smaller or greater than the given `window` size. 

As an example of how useful this can be, in the following example we plot the daily unique interactions of `Lome` via `matplotlib` in only 10 lines. 

=== ":fontawesome-brands-python: Python"

    ```python
    importance = []
    time = []
    for rolling_lome in g.node("LOME").rolling("1 day"):
        importance.append(rolling_lome.degree())
        time.append(rolling_lome.end_date_time)

    plt.plot(time, importance, marker="o")
    plt.xlabel("Date")
    plt.xticks(rotation=45)
    plt.ylabel("Daily Unique Interactions")
    plt.title("Lome's daily interaction count")
    plt.grid(True)
    ```

![lomesDailyInteractions](/assets/images/lomesDailyInteractions.svg)