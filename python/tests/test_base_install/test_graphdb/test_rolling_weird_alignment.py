from datetime import datetime, timezone
import pytest
from raphtory import Graph, GraphView
from utils import run_group_graphql_test

# previously, month boundaries could offset all following rolling windows.
# eg. january 31st + "1 month" = february 28th (to not spill into next month)
# these tests ensure the following windows aren't offset, eg. here on march 28th, april 28th, ...
# rolling windows are end aligned, so we make sure the ends are at the end of the month. the starts may vary based on the window
def test_31st_lines_up_on_30th():
    g: Graph = Graph()
    dt1 = datetime(2025, 3, 31, 14, 37, 52)  # March 31
    dt2 = datetime(2025, 7, 8, 9, 12, 5)  # July 8

    g.add_node(dt1, 1)
    g.add_node(dt2, 1)

    window: list[GraphView] = list(g.rolling("1 month", alignment_unit="day"))[:3]
    window_ms: list[GraphView] = list(g.rolling("1 month", alignment_unit="millisecond"))[:3]

    # April only has 30 days, so ends are on 30th. The window is 1 month so the start is 1 month ago.
    assert window[0].start_date_time == datetime(2025, 3, 30, 0, 0, tzinfo=timezone.utc)
    assert window[0].end_date_time == datetime(2025, 4, 30, 0, 0, tzinfo=timezone.utc)
    assert window_ms[0].start_date_time == datetime(2025, 3, 30, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[0].end_date_time == datetime(2025, 4, 30, 14, 37, 52, tzinfo=timezone.utc)

    # importantly, the ends here are on the 31st instead if 30th
    assert window[1].start_date_time == datetime(2025, 4, 30, 0, 0, tzinfo=timezone.utc)
    assert window[1].end_date_time == datetime(2025, 5, 31, 0, 0, tzinfo=timezone.utc)
    assert window_ms[1].start_date_time == datetime(2025, 4, 30, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[1].end_date_time == datetime(2025, 5, 31, 14, 37, 52, tzinfo=timezone.utc)

    # ends go back to the 30th
    assert window[2].start_date_time == datetime(2025, 5, 30, 0, 0, tzinfo=timezone.utc)
    assert window[2].end_date_time == datetime(2025, 6, 30, 0, 0, tzinfo=timezone.utc)
    assert window_ms[2].start_date_time == datetime(2025, 5, 30, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[2].end_date_time == datetime(2025, 6, 30, 14, 37, 52, tzinfo=timezone.utc)

def test_30th_never_lines_up_on_31st():
    g: Graph = Graph()
    dt1 = datetime(2025, 4, 30, 14, 37, 52)  # April 31
    dt2 = datetime(2025, 7, 8, 9, 12, 5)  # July 8

    g.add_node(dt1, 1)
    g.add_node(dt2, 1)

    window: list[GraphView] = list(g.rolling("1 month", alignment_unit="day"))[:3]
    window_ms: list[GraphView] = list(g.rolling("1 month", alignment_unit="millisecond"))[:3]

    # never line up on 31st if the first event is on the 30th
    assert window[0].start_date_time == datetime(2025, 4, 30, 0, 0, tzinfo=timezone.utc)
    assert window[0].end_date_time == datetime(2025, 5, 30, 0, 0, tzinfo=timezone.utc)
    assert window_ms[0].start_date_time == datetime(2025, 4, 30, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[0].end_date_time == datetime(2025, 5, 30, 14, 37, 52, tzinfo=timezone.utc)

    assert window[1].start_date_time == datetime(2025, 5, 30, 0, 0, tzinfo=timezone.utc)
    assert window[1].end_date_time == datetime(2025, 6, 30, 0, 0, tzinfo=timezone.utc)
    assert window_ms[1].start_date_time == datetime(2025, 5, 30, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[1].end_date_time == datetime(2025, 6, 30, 14, 37, 52, tzinfo=timezone.utc)

    assert window[2].start_date_time == datetime(2025, 6, 30, 0, 0, tzinfo=timezone.utc)
    assert window[2].end_date_time == datetime(2025, 7, 30, 0, 0, tzinfo=timezone.utc)
    assert window_ms[2].start_date_time == datetime(2025, 6, 30, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[2].end_date_time == datetime(2025, 7, 30, 14, 37, 52, tzinfo=timezone.utc)


def test_31st_lines_up_on_31st_july():
    # July 31st
    g: Graph = Graph()
    dt1 = datetime(2025, 7, 31, 14, 37, 52)  # July 31
    dt2 = datetime(2026, 7, 8, 9, 12, 5)  # July 8

    g.add_node(dt1, 1)
    g.add_node(dt2, 1)

    window: list[GraphView] = list(g.rolling("1 month", alignment_unit="day"))[:3]
    window_ms: list[GraphView] = list(g.rolling("1 month", alignment_unit="millisecond"))[:3]

    # starts and ends both 31st because july and august both have 31 days
    assert window[0].start_date_time == datetime(2025, 7, 31, 0, 0, 0, tzinfo=timezone.utc)
    assert window[0].end_date_time == datetime(2025, 8, 31, 0, 0, 0, tzinfo=timezone.utc)
    assert window_ms[0].start_date_time == datetime(2025, 7, 31, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[0].end_date_time == datetime(2025, 8, 31, 14, 37, 52, tzinfo=timezone.utc)

    # ends are now 30th so starts as well
    assert window[1].start_date_time == datetime(2025, 8, 30, 0, 0, 0, tzinfo=timezone.utc)
    assert window[1].end_date_time == datetime(2025, 9, 30, 0, 0, 0, tzinfo=timezone.utc)
    assert window_ms[1].start_date_time == datetime(2025, 8, 30, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[1].end_date_time == datetime(2025, 9, 30, 14, 37, 52, tzinfo=timezone.utc)

    # ends go back to 31st, but start months only have 30 days (October 31st - "1 month" = September 30th) september only has 30 days
    assert window[2].start_date_time == datetime(2025, 9, 30, 0, 0, 0, tzinfo=timezone.utc)
    assert window[2].end_date_time == datetime(2025, 10, 31, 0, 0, 0, tzinfo=timezone.utc)
    assert window_ms[2].start_date_time == datetime(2025, 9, 30, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[2].end_date_time == datetime(2025, 10, 31, 14, 37, 52, tzinfo=timezone.utc)


def test_31st_lines_up_on_28th_february():
    # starting on December 31st
    g: Graph = Graph()
    dt1 = datetime(2025, 12, 31, 14, 37, 52)  # December 31
    dt2 = datetime(2026, 7, 8, 9, 12, 5)  # July 8

    g.add_node(dt1, 1)
    g.add_node(dt2, 1)

    window: list[GraphView] = list(g.rolling("1 month", alignment_unit="day"))[:3]
    window_ms: list[GraphView] = list(g.rolling("1 month", alignment_unit="millisecond"))[:3]

    # december and january both have 31 days so start and end are both on the 31st
    assert window[0].start_date_time == datetime(2025, 12, 31, 0, 0, 0, tzinfo=timezone.utc)
    assert window[0].end_date_time == datetime(2026, 1, 31, 0, 0, tzinfo=timezone.utc)
    assert window_ms[0].start_date_time == datetime(2025, 12, 31, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[0].end_date_time == datetime(2026, 1, 31, 14, 37, 52, tzinfo=timezone.utc)

    # february has 28 days so the ends line up on the 28th, and the starts follow
    assert window[1].start_date_time == datetime(2026, 1, 28, 0, 0, 0, tzinfo=timezone.utc)
    assert window[1].end_date_time == datetime(2026, 2, 28, 0, 0, tzinfo=timezone.utc)
    assert window_ms[1].start_date_time == datetime(2026, 1, 28, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[1].end_date_time == datetime(2026, 2, 28, 14, 37, 52, tzinfo=timezone.utc)

    # march has 31 days, but March 31st - "1 month" = February 28th (feb 31st doesn't exist)
    assert window[2].start_date_time == datetime(2026, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    assert window[2].end_date_time == datetime(2026, 3, 31, 0, 0, tzinfo=timezone.utc)
    assert window_ms[2].start_date_time == datetime(2026, 2, 28, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[2].end_date_time == datetime(2026, 3, 31, 14, 37, 52, tzinfo=timezone.utc)

    # starting on January 31st
    g = Graph()
    dt1 = datetime(2025, 1, 31, 14, 37, 52)  # January 31st
    dt2 = datetime(2026, 7, 8, 9, 12, 5)  # July 8

    g.add_node(dt1, 1)
    g.add_node(dt2, 1)

    window: list[GraphView] = list(g.rolling("1 month", alignment_unit="day"))[:3]
    window_ms: list[GraphView] = list(g.rolling("1 month", alignment_unit="millisecond"))[:3]

    # february has 28 days so the ends line up on the 28th, and the starts follow
    assert window[0].start_date_time == datetime(2025, 1, 28, 0, 0, 0, tzinfo=timezone.utc)
    assert window[0].end_date_time == datetime(2025, 2, 28, 0, 0, tzinfo=timezone.utc)
    assert window_ms[0].start_date_time == datetime(2025, 1, 28, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[0].end_date_time == datetime(2025, 2, 28, 14, 37, 52, tzinfo=timezone.utc)

    # march has 31 days, but March 31st - "1 month" = February 28th (feb 31st doesn't exist)
    assert window[1].start_date_time == datetime(2025, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    assert window[1].end_date_time == datetime(2025, 3, 31, 0, 0, tzinfo=timezone.utc)
    assert window_ms[1].start_date_time == datetime(2025, 2, 28, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[1].end_date_time == datetime(2025, 3, 31, 14, 37, 52, tzinfo=timezone.utc)

    # ends are now lined on 30th (April has 30 days)
    assert window[2].start_date_time == datetime(2025, 3, 30, 0, 0, 0, tzinfo=timezone.utc)
    assert window[2].end_date_time == datetime(2025, 4, 30, 0, 0, tzinfo=timezone.utc)
    assert window_ms[2].start_date_time == datetime(2025, 3, 30, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[2].end_date_time == datetime(2025, 4, 30, 14, 37, 52, tzinfo=timezone.utc)


def test_31st_lines_up_on_29th_february():
    # starting on December 31st
    g: Graph = Graph()
    dt1 = datetime(2023, 12, 31, 14, 37, 52)  # December 31
    dt2 = datetime(2024, 7, 8, 9, 12, 5)  # July 8

    g.add_node(dt1, 1)
    g.add_node(dt2, 1)

    window: list[GraphView] = list(g.rolling("1 month", alignment_unit="day"))[:3]
    window_ms: list[GraphView] = list(g.rolling("1 month", alignment_unit="millisecond"))[:3]

    # december and january both have 31 days so start and end are both on the 31st
    assert window[0].start_date_time == datetime(2023, 12, 31, 0, 0, 0, tzinfo=timezone.utc)
    assert window[0].end_date_time == datetime(2024, 1, 31, 0, 0, tzinfo=timezone.utc)
    assert window_ms[0].start_date_time == datetime(2023, 12, 31, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[0].end_date_time == datetime(2024, 1, 31, 14, 37, 52, tzinfo=timezone.utc)

    # leap year february has 29 days so the ends line up on the 29th, and the starts follow
    assert window[1].start_date_time == datetime(2024, 1, 29, 0, 0, 0, tzinfo=timezone.utc)
    assert window[1].end_date_time == datetime(2024, 2, 29, 0, 0, tzinfo=timezone.utc)
    assert window_ms[1].start_date_time == datetime(2024, 1, 29, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[1].end_date_time == datetime(2024, 2, 29, 14, 37, 52, tzinfo=timezone.utc)

    # march has 31 days, but March 31st - "1 month" = February 29th (feb 31st doesn't exist)
    assert window[2].start_date_time == datetime(2024, 2, 29, 0, 0, 0, tzinfo=timezone.utc)
    assert window[2].end_date_time == datetime(2024, 3, 31, 0, 0, tzinfo=timezone.utc)
    assert window_ms[2].start_date_time == datetime(2024, 2, 29, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[2].end_date_time == datetime(2024, 3, 31, 14, 37, 52, tzinfo=timezone.utc)

    # starting on January 31st
    g = Graph()
    dt1 = datetime(2024, 1, 31, 14, 37, 52)  # January 31st
    dt2 = datetime(2026, 7, 8, 9, 12, 5)  # July 8

    g.add_node(dt1, 1)
    g.add_node(dt2, 1)

    window: list[GraphView] = list(g.rolling("1 month", alignment_unit="day"))[:3]
    window_ms: list[GraphView] = list(g.rolling("1 month", alignment_unit="millisecond"))[:3]

    # leap year february has 29 days so the ends line up on the 29th, and the starts follow
    assert window[0].start_date_time == datetime(2024, 1, 29, 0, 0, 0, tzinfo=timezone.utc)
    assert window[0].end_date_time == datetime(2024, 2, 29, 0, 0, tzinfo=timezone.utc)
    assert window_ms[0].start_date_time == datetime(2024, 1, 29, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[0].end_date_time == datetime(2024, 2, 29, 14, 37, 52, tzinfo=timezone.utc)

    # march has 31 days, but March 31st - "1 month" = February 29th (feb 31st doesn't exist)
    assert window[1].start_date_time == datetime(2024, 2, 29, 0, 0, 0, tzinfo=timezone.utc)
    assert window[1].end_date_time == datetime(2024, 3, 31, 0, 0, tzinfo=timezone.utc)
    assert window_ms[1].start_date_time == datetime(2024, 2, 29, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[1].end_date_time == datetime(2024, 3, 31, 14, 37, 52, tzinfo=timezone.utc)

    # ends are now lined on 30th (April has 30 days)
    assert window[2].start_date_time == datetime(2024, 3, 30, 0, 0, 0, tzinfo=timezone.utc)
    assert window[2].end_date_time == datetime(2024, 4, 30, 0, 0, tzinfo=timezone.utc)
    assert window_ms[2].start_date_time == datetime(2024, 3, 30, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[2].end_date_time == datetime(2024, 4, 30, 14, 37, 52, tzinfo=timezone.utc)


def test_feb_28th_start_alignment_with_step():
    g: Graph = Graph()
    dt1 = datetime(2025, 3, 15, 14, 37, 52)  # March 15
    dt2 = datetime(2025, 7, 8, 9, 12, 5)  # July 8
    dt3 = datetime(2025, 11, 22, 21, 45, 30)  # November 22

    g.add_node(dt1, 1)
    g.add_node(dt2, 1)
    g.add_node(dt3, 1)

    window_1_week: list[GraphView] = list(g.rolling("1 month", step="1 week", alignment_unit="day"))[:2]
    window_1_week_ms: list[GraphView] = list(g.rolling("1 month", step="1 week", alignment_unit="millisecond"))[:2]
    window_2_weeks: list[GraphView] = list(g.rolling("1 month", step="2 weeks", alignment_unit="day"))[:2]
    window_2_weeks_ms: list[GraphView] = list(g.rolling("1 month", step="2 weeks", alignment_unit="millisecond"))[:2]

    # start aligns to february 28th because March 15th + "2 weeks" = March 29th - "1 month" = february 28th
    assert window_1_week[0].start_date_time == datetime(2025, 2, 22, 0, 0, 0, tzinfo=timezone.utc)
    assert window_1_week[0].end_date_time == datetime(2025, 3, 22, 0, 0, 0, tzinfo=timezone.utc)
    assert window_1_week_ms[0].start_date_time == datetime(2025, 2, 22, 14, 37, 52, tzinfo=timezone.utc)
    assert window_1_week_ms[0].end_date_time == datetime(2025, 3, 22, 14, 37, 52, tzinfo=timezone.utc)

    assert window_2_weeks[0].start_date_time == datetime(2025, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    assert window_2_weeks[0].end_date_time == datetime(2025, 3, 29, 0, 0, 0, tzinfo=timezone.utc)
    assert window_2_weeks_ms[0].start_date_time == datetime(2025, 2, 28, 14, 37, 52, tzinfo=timezone.utc)
    assert window_2_weeks_ms[0].end_date_time == datetime(2025, 3, 29, 14, 37, 52, tzinfo=timezone.utc)

    assert window_1_week[1].start_date_time == datetime(2025, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    assert window_1_week[1].end_date_time == datetime(2025, 3, 29, 0, 0, 0, tzinfo=timezone.utc)
    assert window_1_week_ms[1].start_date_time == datetime(2025, 2, 28, 14, 37, 52, tzinfo=timezone.utc)
    assert window_1_week_ms[1].end_date_time == datetime(2025, 3, 29, 14, 37, 52, tzinfo=timezone.utc)

    assert window_2_weeks[1].start_date_time == datetime(2025, 3, 12, 0, 0, 0, tzinfo=timezone.utc)
    assert window_2_weeks[1].end_date_time == datetime(2025, 4, 12, 0, 0, 0, tzinfo=timezone.utc)
    assert window_2_weeks_ms[1].start_date_time == datetime(2025, 3, 12, 14, 37, 52, tzinfo=timezone.utc)
    assert window_2_weeks_ms[1].end_date_time == datetime(2025, 4, 12, 14, 37, 52, tzinfo=timezone.utc)


def test_feb_29th_start_alignment_with_step():
    g: Graph = Graph()
    dt1 = datetime(2024, 3, 17, 14, 37, 52)  # May 17
    dt2 = datetime(2024, 7, 8, 9, 12, 5)  # July 8
    dt3 = datetime(2024, 11, 22, 21, 45, 30)  # November 22

    g.add_node(dt1, 1)
    g.add_node(dt2, 1)
    g.add_node(dt3, 1)

    window_1_week: list[GraphView] = list(g.rolling("1 month", step="1 week", alignment_unit="day"))[:2]
    window_1_week_ms: list[GraphView] = list(g.rolling("1 month", step="1 week", alignment_unit="millisecond"))[:2]
    window_2_weeks: list[GraphView] = list(g.rolling("1 month", step="2 weeks", alignment_unit="day"))[:2]
    window_2_weeks_ms: list[GraphView] = list(g.rolling("1 month", step="2 weeks", alignment_unit="millisecond"))[:2]

    assert window_1_week[0].start_date_time == datetime(2024, 2, 24, 0, 0, 0, tzinfo=timezone.utc)
    assert window_1_week[0].end_date_time == datetime(2024, 3, 24, 0, 0, 0, tzinfo=timezone.utc)
    assert window_1_week_ms[0].start_date_time == datetime(2024, 2, 24, 14, 37, 52, tzinfo=timezone.utc)
    assert window_1_week_ms[0].end_date_time == datetime(2024, 3, 24, 14, 37, 52, tzinfo=timezone.utc)
    # march 31st - "1 month" = February 29th (leap year)
    assert window_2_weeks[0].start_date_time == datetime(2024, 2, 29, 0, 0, 0, tzinfo=timezone.utc)
    assert window_2_weeks[0].end_date_time == datetime(2024, 3, 31, 0, 0, 0, tzinfo=timezone.utc)
    assert window_2_weeks_ms[0].start_date_time == datetime(2024, 2, 29, 14, 37, 52, tzinfo=timezone.utc)
    assert window_2_weeks_ms[0].end_date_time == datetime(2024, 3, 31, 14, 37, 52, tzinfo=timezone.utc)

    assert window_1_week[1].start_date_time == datetime(2024, 2, 29, 0, 0, 0, tzinfo=timezone.utc)
    assert window_1_week[1].end_date_time == datetime(2024, 3, 31, 0, 0, 0, tzinfo=timezone.utc)
    assert window_1_week_ms[1].start_date_time == datetime(2024, 2, 29, 14, 37, 52, tzinfo=timezone.utc)
    assert window_1_week_ms[1].end_date_time == datetime(2024, 3, 31, 14, 37, 52, tzinfo=timezone.utc)

    assert window_2_weeks[1].start_date_time == datetime(2024, 3, 14, 0, 0, 0, tzinfo=timezone.utc)
    assert window_2_weeks[1].end_date_time == datetime(2024, 4, 14, 0, 0, 0, tzinfo=timezone.utc)
    assert window_2_weeks_ms[1].start_date_time == datetime(2024, 3, 14, 14, 37, 52, tzinfo=timezone.utc)
    assert window_2_weeks_ms[1].end_date_time == datetime(2024, 4, 14, 14, 37, 52, tzinfo=timezone.utc)


def test_feb_28th_no_weirdness():
    g: Graph = Graph()
    dt1 = datetime(2025, 2, 28, 14, 37, 52)  # February 28
    dt2 = datetime(2025, 7, 8, 9, 12, 5)  # July 8

    g.add_node(dt1, 1)
    g.add_node(dt2, 1)

    window: list[GraphView] = list(g.rolling("1 month", alignment_unit="day"))[:3]
    window_ms: list[GraphView] = list(g.rolling("1 month", alignment_unit="millisecond"))[:3]

    assert window[0].start_date_time == datetime(2025, 2, 28, 0, 0, 0, tzinfo=timezone.utc)
    assert window[0].end_date_time == datetime(2025, 3, 28, 0, 0, 0, tzinfo=timezone.utc)
    assert window_ms[0].start_date_time == datetime(2025, 2, 28, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[0].end_date_time == datetime(2025, 3, 28, 14, 37, 52, tzinfo=timezone.utc)

    # no weirdness
    assert window[1].start_date_time == datetime(2025, 3, 28, 0, 0, 0, tzinfo=timezone.utc)
    assert window[1].end_date_time == datetime(2025, 4, 28, 0, 0, 0, tzinfo=timezone.utc)
    assert window_ms[1].start_date_time == datetime(2025, 3, 28, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[1].end_date_time == datetime(2025, 4, 28, 14, 37, 52, tzinfo=timezone.utc)

def test_feb_29th_no_weirdness():
    g: Graph = Graph()
    dt1 = datetime(2024, 2, 29, 14, 37, 52)  # February 29
    dt2 = datetime(2024, 7, 8, 9, 12, 5)  # July 8

    g.add_node(dt1, 1)
    g.add_node(dt2, 1)

    window: list[GraphView] = list(g.rolling("1 month", alignment_unit="day"))[:3]
    window_ms: list[GraphView] = list(g.rolling("1 month", alignment_unit="millisecond"))[:3]

    assert window[0].start_date_time == datetime(2024, 2, 29, 0, 0, 0, tzinfo=timezone.utc)
    assert window[0].end_date_time == datetime(2024, 3, 29, 0, 0, 0, tzinfo=timezone.utc)
    assert window_ms[0].start_date_time == datetime(2024, 2, 29, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[0].end_date_time == datetime(2024, 3, 29, 14, 37, 52, tzinfo=timezone.utc)

    # no weirdness
    assert window[1].start_date_time == datetime(2024, 3, 29, 0, 0, 0, tzinfo=timezone.utc)
    assert window[1].end_date_time == datetime(2024, 4, 29, 0, 0, 0, tzinfo=timezone.utc)
    assert window_ms[1].start_date_time == datetime(2024, 3, 29, 14, 37, 52, tzinfo=timezone.utc)
    assert window_ms[1].end_date_time == datetime(2024, 4, 29, 14, 37, 52, tzinfo=timezone.utc)