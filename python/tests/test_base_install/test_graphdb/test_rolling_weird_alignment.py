from datetime import datetime, timezone
import pytest
from raphtory import Graph, GraphView
from utils import run_group_graphql_test


def test_31st_lines_up_on_30th():
    g: Graph = Graph()
    dt1 = datetime(2025, 3, 31, 14, 37, 52)  # March 31
    dt2 = datetime(2025, 7, 8, 9, 12, 5)  # July 8

    g.add_node(dt1, 1)
    g.add_node(dt2, 1)

    window_only: list[GraphView] = list(g.rolling("1 month", alignment_unit="day"))[:2]
    window_only_ms: list[GraphView] = list(
        g.rolling("1 month", alignment_unit="millisecond")
    )[:2]
    window_step: list[GraphView] = list(
        g.rolling("1 month", step="1 week", alignment_unit="day")
    )[:2]
    window_step_ms: list[GraphView] = list(
        g.rolling("1 month", step="1 week", alignment_unit="millisecond")
    )[:2]

    for i in range(2):
        print(
            f"\nWINDOW ONLY:        Start: {window_only[i].start_date_time}; End: {window_only[i].end_date_time}"
        )
        print(
            f"WINDOW ONLY MS:     Start: {window_only_ms[i].start_date_time}; End: {window_only_ms[i].end_date_time}"
        )
        print(
            f"WINDOW & STEP:      Start: {window_step[i].start_date_time}; End: {window_step[i].end_date_time}"
        )
        print(
            f"WINDOW & STEP MS:   Start: {window_step_ms[i].start_date_time}; End: {window_step_ms[i].end_date_time}"
        )
    # Output
    # WINDOW ONLY:        Start: 2025-03-30 00:00:00+00:00; End: 2025-04-30 00:00:00+00:00
    # WINDOW ONLY MS:     Start: 2025-03-30 14:37:52+00:00; End: 2025-04-30 14:37:52+00:00
    # WINDOW & STEP:      Start: 2025-03-07 00:00:00+00:00; End: 2025-04-07 00:00:00+00:00
    # WINDOW & STEP MS:   Start: 2025-03-07 14:37:52+00:00; End: 2025-04-07 14:37:52+00:00
    #
    # WINDOW ONLY:        Start: 2025-04-30 00:00:00+00:00; End: 2025-05-30 00:00:00+00:00
    # WINDOW ONLY MS:     Start: 2025-04-30 14:37:52+00:00; End: 2025-05-30 14:37:52+00:00
    # WINDOW & STEP:      Start: 2025-03-14 00:00:00+00:00; End: 2025-04-14 00:00:00+00:00
    # WINDOW & STEP MS:   Start: 2025-03-14 14:37:52+00:00; End: 2025-04-14 14:37:52+00:00


def test_31st_lines_up_on_31st_december_july():
    print("\n###########JULY 31ST###########")
    g: Graph = Graph()
    dt1 = datetime(2025, 7, 31, 14, 37, 52)  # July 31
    dt2 = datetime(2026, 7, 8, 9, 12, 5)  # July 8

    g.add_node(dt1, 1)
    g.add_node(dt2, 1)

    window_only: list[GraphView] = list(g.rolling("1 month", alignment_unit="day"))[:2]
    window_only_ms: list[GraphView] = list(
        g.rolling("1 month", alignment_unit="millisecond")
    )[:2]
    window_step: list[GraphView] = list(
        g.rolling("1 month", step="1 week", alignment_unit="day")
    )[:2]
    window_step_ms: list[GraphView] = list(
        g.rolling("1 month", step="1 week", alignment_unit="millisecond")
    )[:2]

    for i in range(2):
        print(
            f"WINDOW ONLY:        Start: {window_only[i].start_date_time}; End: {window_only[i].end_date_time}"
        )
        print(
            f"WINDOW ONLY MS:     Start: {window_only_ms[i].start_date_time}; End: {window_only_ms[i].end_date_time}"
        )
        print(
            f"WINDOW & STEP:      Start: {window_step[i].start_date_time}; End: {window_step[i].end_date_time}"
        )
        print(
            f"WINDOW & STEP MS:   Start: {window_step_ms[i].start_date_time}; End: {window_step_ms[i].end_date_time}\n"
        )

    # Output
    # WINDOW ONLY:        Start: 2025-07-31 00:00:00+00:00; End: 2025-08-31 00:00:00+00:00
    # WINDOW ONLY MS:     Start: 2025-07-31 14:37:52+00:00; End: 2025-08-31 14:37:52+00:00
    # WINDOW & STEP:      Start: 2025-07-07 00:00:00+00:00; End: 2025-08-07 00:00:00+00:00
    # WINDOW & STEP MS:   Start: 2025-07-07 14:37:52+00:00; End: 2025-08-07 14:37:52+00:00
    #
    # WINDOW ONLY:        Start: 2025-08-30 00:00:00+00:00; End: 2025-09-30 00:00:00+00:00
    # WINDOW ONLY MS:     Start: 2025-08-30 14:37:52+00:00; End: 2025-09-30 14:37:52+00:00
    # WINDOW & STEP:      Start: 2025-07-14 00:00:00+00:00; End: 2025-08-14 00:00:00+00:00
    # WINDOW & STEP MS:   Start: 2025-07-14 14:37:52+00:00; End: 2025-08-14 14:37:52+00:00

    print("\n###########DECEMBER 31ST###########")
    g: Graph = Graph()
    dt1 = datetime(2025, 12, 31, 14, 37, 52)  # December 31
    dt2 = datetime(2026, 7, 8, 9, 12, 5)  # July 8

    g.add_node(dt1, 1)
    g.add_node(dt2, 1)

    window_only: list[GraphView] = list(g.rolling("1 month", alignment_unit="day"))[:2]
    window_only_ms: list[GraphView] = list(
        g.rolling("1 month", alignment_unit="millisecond")
    )[:2]
    window_step: list[GraphView] = list(
        g.rolling("1 month", step="1 week", alignment_unit="day")
    )[:2]
    window_step_ms: list[GraphView] = list(
        g.rolling("1 month", step="1 week", alignment_unit="millisecond")
    )[:2]

    for i in range(2):
        print(
            f"WINDOW ONLY:        Start: {window_only[i].start_date_time}; End: {window_only[i].end_date_time}"
        )
        print(
            f"WINDOW ONLY MS:     Start: {window_only_ms[i].start_date_time}; End: {window_only_ms[i].end_date_time}"
        )
        print(
            f"WINDOW & STEP:      Start: {window_step[i].start_date_time}; End: {window_step[i].end_date_time}"
        )
        print(
            f"WINDOW & STEP MS:   Start: {window_step_ms[i].start_date_time}; End: {window_step_ms[i].end_date_time}\n"
        )

    # Output
    # WINDOW ONLY:        Start: 2025-12-31 00:00:00+00:00; End: 2026-01-31 00:00:00+00:00
    # WINDOW ONLY MS:     Start: 2025-12-31 14:37:52+00:00; End: 2026-01-31 14:37:52+00:00
    # WINDOW & STEP:      Start: 2025-12-07 00:00:00+00:00; End: 2026-01-07 00:00:00+00:00
    # WINDOW & STEP MS:   Start: 2025-12-07 14:37:52+00:00; End: 2026-01-07 14:37:52+00:00
    #
    # WINDOW ONLY:        Start: 2026-01-28 00:00:00+00:00; End: 2026-02-28 00:00:00+00:00
    # WINDOW ONLY MS:     Start: 2026-01-28 14:37:52+00:00; End: 2026-02-28 14:37:52+00:00
    # WINDOW & STEP:      Start: 2025-12-14 00:00:00+00:00; End: 2026-01-14 00:00:00+00:00
    # WINDOW & STEP MS:   Start: 2025-12-14 14:37:52+00:00; End: 2026-01-14 14:37:52+00:00


def test_31st_lines_up_on_28th_january():
    print("\n###########JANUARY 31ST###########")
    g = Graph()
    dt1 = datetime(2025, 1, 31, 14, 37, 52)  # January 31st
    dt2 = datetime(2026, 7, 8, 9, 12, 5)  # July 8

    g.add_node(dt1, 1)
    g.add_node(dt2, 1)

    window_only: list[GraphView] = list(g.rolling("1 month", alignment_unit="day"))[:4]
    window_only_ms: list[GraphView] = list(
        g.rolling("1 month", alignment_unit="millisecond")
    )[:4]
    window_step: list[GraphView] = list(
        g.rolling("1 month", step="1 week", alignment_unit="day")
    )[:4]
    window_step_ms: list[GraphView] = list(
        g.rolling("1 month", step="1 week", alignment_unit="millisecond")
    )[:4]

    for i in range(4):
        print(
            f"WINDOW ONLY:        Start: {window_only[i].start_date_time}; End: {window_only[i].end_date_time}"
        )
        print(
            f"WINDOW ONLY MS:     Start: {window_only_ms[i].start_date_time}; End: {window_only_ms[i].end_date_time}"
        )
        print(
            f"WINDOW & STEP:      Start: {window_step[i].start_date_time}; End: {window_step[i].end_date_time}"
        )
        print(
            f"WINDOW & STEP MS:   Start: {window_step_ms[i].start_date_time}; End: {window_step_ms[i].end_date_time}\n"
        )

    # Output
    # WINDOW ONLY:        Start: 2025-01-28 00:00:00+00:00; End: 2025-02-28 00:00:00+00:00
    # WINDOW ONLY MS:     Start: 2025-01-28 14:37:52+00:00; End: 2025-02-28 14:37:52+00:00
    # WINDOW & STEP:      Start: 2025-01-07 00:00:00+00:00; End: 2025-02-07 00:00:00+00:00
    # WINDOW & STEP MS:   Start: 2025-01-07 14:37:52+00:00; End: 2025-02-07 14:37:52+00:00
    #
    # WINDOW ONLY:        Start: 2025-02-28 00:00:00+00:00; End: 2025-03-28 00:00:00+00:00
    # WINDOW ONLY MS:     Start: 2025-02-28 14:37:52+00:00; End: 2025-03-28 14:37:52+00:00
    # WINDOW & STEP:      Start: 2025-01-14 00:00:00+00:00; End: 2025-02-14 00:00:00+00:00
    # WINDOW & STEP MS:   Start: 2025-01-14 14:37:52+00:00; End: 2025-02-14 14:37:52+00:00


def test_feb_28th_extra_day_with_step():
    g: Graph = Graph()
    dt1 = datetime(2025, 3, 15, 14, 37, 52)  # March 15
    dt2 = datetime(2025, 7, 8, 9, 12, 5)  # July 8
    dt3 = datetime(2025, 11, 22, 21, 45, 30)  # November 22

    g.add_node(dt1, 1)
    g.add_node(dt2, 1)
    g.add_node(dt3, 1)

    window_only: list[GraphView] = list(g.rolling("1 month", alignment_unit="day"))[:2]
    window_step_1_week: list[GraphView] = list(
        g.rolling("1 month", step="1 week", alignment_unit="day")
    )[:2]
    window_step_1_week_ms: list[GraphView] = list(
        g.rolling("1 month", step="1 week", alignment_unit="millisecond")
    )[:2]
    window_step_2_weeks: list[GraphView] = list(
        g.rolling("1 month", step="2 weeks", alignment_unit="day")
    )[:2]
    window_step_2_weeks_ms: list[GraphView] = list(
        g.rolling("1 month", step="2 weeks", alignment_unit="millisecond")
    )[:2]

    for i in range(2):
        print(
            f"\nWINDOW ONLY:                Start: {window_only[i].start_date_time}; End: {window_only[i].end_date_time}"
        )
        print(
            f"WINDOW & 1 WEEK STEP:       Start: {window_step_1_week[i].start_date_time}; End: {window_step_1_week[i].end_date_time}"
        )
        print(
            f"WINDOW & 1 WEEK STEP MS:    Start: {window_step_1_week_ms[i].start_date_time}; End: {window_step_1_week_ms[i].end_date_time}"
        )
        print(
            f"WINDOW & 2 WEEK STEP:       Start: {window_step_2_weeks[i].start_date_time}; End: {window_step_2_weeks[i].end_date_time}"
        )
        print(
            f"WINDOW & 2 WEEK STEP MS:    Start: {window_step_2_weeks_ms[i].start_date_time}; End: {window_step_2_weeks_ms[i].end_date_time}"
        )

    # Output
    # WINDOW ONLY:                Start: 2025-03-15 00:00:00+00:00; End: 2025-04-15 00:00:00+00:00
    # WINDOW & 1 WEEK STEP:       Start: 2025-02-22 00:00:00+00:00; End: 2025-03-22 00:00:00+00:00
    # WINDOW & 1 WEEK STEP MS:    Start: 2025-02-22 14:37:52+00:00; End: 2025-03-22 14:37:52+00:00
    # WINDOW & 2 WEEK STEP:       Start: 2025-02-28 00:00:00+00:00; End: 2025-03-29 00:00:00+00:00
    # WINDOW & 2 WEEK STEP MS:    Start: 2025-02-28 14:37:52+00:00; End: 2025-03-29 14:37:52+00:00
    #
    # WINDOW ONLY:                Start: 2025-04-15 00:00:00+00:00; End: 2025-05-15 00:00:00+00:00
    # WINDOW & 1 WEEK STEP:       Start: 2025-02-28 00:00:00+00:00; End: 2025-03-29 00:00:00+00:00
    # WINDOW & 1 WEEK STEP MS:    Start: 2025-02-28 14:37:52+00:00; End: 2025-03-29 14:37:52+00:00
    # WINDOW & 2 WEEK STEP:       Start: 2025-03-12 00:00:00+00:00; End: 2025-04-12 00:00:00+00:00
    # WINDOW & 2 WEEK STEP MS:    Start: 2025-03-12 14:37:52+00:00; End: 2025-04-12 14:37:52+00:00


def test_feb_28th_no_extra_day_without_step():
    g: Graph = Graph()
    dt1 = datetime(2025, 2, 28, 14, 37, 52)  # February 28
    dt2 = datetime(2025, 7, 8, 9, 12, 5)  # July 8

    g.add_node(dt1, 1)
    g.add_node(dt2, 1)

    window_only: list[GraphView] = list(g.rolling("1 month", alignment_unit="day"))[:2]
    window_only_ms: list[GraphView] = list(
        g.rolling("1 month", alignment_unit="millisecond")
    )[:2]
    window_step: list[GraphView] = list(
        g.rolling("1 month", step="1 week", alignment_unit="day")
    )[:2]
    window_step_ms: list[GraphView] = list(
        g.rolling("1 month", step="1 week", alignment_unit="millisecond")
    )[:2]

    for i in range(2):
        print(
            f"\nWINDOW ONLY:        Start: {window_only[i].start_date_time}; End: {window_only[i].end_date_time}"
        )
        print(
            f"WINDOW ONLY MS:     Start: {window_only_ms[i].start_date_time}; End: {window_only_ms[i].end_date_time}"
        )
        print(
            f"WINDOW & STEP:      Start: {window_step[i].start_date_time}; End: {window_step[i].end_date_time}"
        )
        print(
            f"WINDOW & STEP MS:   Start: {window_step_ms[i].start_date_time}; End: {window_step_ms[i].end_date_time}"
        )

    # Output
    # WINDOW ONLY:        Start: 2025-02-28 00:00:00+00:00; End: 2025-03-28 00:00:00+00:00
    # WINDOW ONLY MS:     Start: 2025-02-28 14:37:52+00:00; End: 2025-03-28 14:37:52+00:00
    # WINDOW & STEP:      Start: 2025-02-07 00:00:00+00:00; End: 2025-03-07 00:00:00+00:00
    # WINDOW & STEP MS:   Start: 2025-02-07 14:37:52+00:00; End: 2025-03-07 14:37:52+00:00
    #
    # WINDOW ONLY:        Start: 2025-03-28 00:00:00+00:00; End: 2025-04-28 00:00:00+00:00
    # WINDOW ONLY MS:     Start: 2025-03-28 14:37:52+00:00; End: 2025-04-28 14:37:52+00:00
    # WINDOW & STEP:      Start: 2025-02-14 00:00:00+00:00; End: 2025-03-14 00:00:00+00:00
    # WINDOW & STEP MS:   Start: 2025-02-14 14:37:52+00:00; End: 2025-03-14 14:37:52+00:00


def test_may_31st_extra_day_with_step():
    g: Graph = Graph()
    dt1 = datetime(2025, 5, 17, 14, 37, 52)  # May 17
    dt2 = datetime(2025, 7, 8, 9, 12, 5)  # July 8
    dt3 = datetime(2025, 11, 22, 21, 45, 30)  # November 22

    g.add_node(dt1, 1)
    g.add_node(dt2, 1)
    g.add_node(dt3, 1)

    window_only: list[GraphView] = list(g.rolling("1 month", alignment_unit="day"))[:2]
    window_step_1_week: list[GraphView] = list(
        g.rolling("1 month", step="1 week", alignment_unit="day")
    )[:2]
    window_step_1_week_ms: list[GraphView] = list(
        g.rolling("1 month", step="1 week", alignment_unit="millisecond")
    )[:2]
    window_step_2_weeks: list[GraphView] = list(
        g.rolling("1 month", step="2 weeks", alignment_unit="day")
    )[:2]
    window_step_2_weeks_ms: list[GraphView] = list(
        g.rolling("1 month", step="2 weeks", alignment_unit="millisecond")
    )[:2]

    for i in range(2):
        print(
            f"\nWINDOW ONLY:                Start: {window_only[i].start_date_time}; End: {window_only[i].end_date_time}"
        )
        print(
            f"WINDOW & 1 WEEK STEP:       Start: {window_step_1_week[i].start_date_time}; End: {window_step_1_week[i].end_date_time}"
        )
        print(
            f"WINDOW & 1 WEEK STEP MS:    Start: {window_step_1_week_ms[i].start_date_time}; End: {window_step_1_week_ms[i].end_date_time}"
        )
        print(
            f"WINDOW & 2 WEEK STEP:       Start: {window_step_2_weeks[i].start_date_time}; End: {window_step_2_weeks[i].end_date_time}"
        )
        print(
            f"WINDOW & 2 WEEK STEP MS:    Start: {window_step_2_weeks_ms[i].start_date_time}; End: {window_step_2_weeks_ms[i].end_date_time}"
        )

    # Output
    # WINDOW ONLY:                Start: 2025-05-17 00:00:00+00:00; End: 2025-06-17 00:00:00+00:00
    # WINDOW & 1 WEEK STEP:       Start: 2025-04-24 00:00:00+00:00; End: 2025-05-24 00:00:00+00:00
    # WINDOW & 1 WEEK STEP MS:    Start: 2025-04-24 14:37:52+00:00; End: 2025-05-24 14:37:52+00:00
    # WINDOW & 2 WEEK STEP:       Start: 2025-04-30 00:00:00+00:00; End: 2025-05-31 00:00:00+00:00
    # WINDOW & 2 WEEK STEP MS:    Start: 2025-04-30 14:37:52+00:00; End: 2025-05-31 14:37:52+00:00
    #
    # WINDOW ONLY:                Start: 2025-06-17 00:00:00+00:00; End: 2025-07-17 00:00:00+00:00
    # WINDOW & 1 WEEK STEP:       Start: 2025-04-30 00:00:00+00:00; End: 2025-05-31 00:00:00+00:00
    # WINDOW & 1 WEEK STEP MS:    Start: 2025-04-30 14:37:52+00:00; End: 2025-05-31 14:37:52+00:00
    # WINDOW & 2 WEEK STEP:       Start: 2025-05-14 00:00:00+00:00; End: 2025-06-14 00:00:00+00:00
    # WINDOW & 2 WEEK STEP MS:    Start: 2025-05-14 14:37:52+00:00; End: 2025-06-14 14:37:52+00:00


def test_march_31st_multiple_extra_days_with_step():
    g: Graph = Graph()
    dt1 = datetime(2025, 3, 17, 14, 37, 52)  # May 17
    dt2 = datetime(2025, 7, 8, 9, 12, 5)  # July 8
    dt3 = datetime(2025, 11, 22, 21, 45, 30)  # November 22

    g.add_node(dt1, 1)
    g.add_node(dt2, 1)
    g.add_node(dt3, 1)

    window_only: list[GraphView] = list(g.rolling("1 month", alignment_unit="day"))[:2]
    window_step_1_week: list[GraphView] = list(
        g.rolling("1 month", step="1 week", alignment_unit="day")
    )[:2]
    window_step_1_week_ms: list[GraphView] = list(
        g.rolling("1 month", step="1 week", alignment_unit="millisecond")
    )[:2]
    window_step_2_weeks: list[GraphView] = list(
        g.rolling("1 month", step="2 weeks", alignment_unit="day")
    )[:2]
    window_step_2_weeks_ms: list[GraphView] = list(
        g.rolling("1 month", step="2 weeks", alignment_unit="millisecond")
    )[:2]

    for i in range(2):
        print(
            f"\nWINDOW ONLY:                Start: {window_only[i].start_date_time}; End: {window_only[i].end_date_time}"
        )
        print(
            f"WINDOW & 1 WEEK STEP:       Start: {window_step_1_week[i].start_date_time}; End: {window_step_1_week[i].end_date_time}"
        )
        print(
            f"WINDOW & 1 WEEK STEP MS:    Start: {window_step_1_week_ms[i].start_date_time}; End: {window_step_1_week_ms[i].end_date_time}"
        )
        print(
            f"WINDOW & 2 WEEK STEP:       Start: {window_step_2_weeks[i].start_date_time}; End: {window_step_2_weeks[i].end_date_time}"
        )
        print(
            f"WINDOW & 2 WEEK STEP MS:    Start: {window_step_2_weeks_ms[i].start_date_time}; End: {window_step_2_weeks_ms[i].end_date_time}"
        )

    # Output
    # WINDOW ONLY:                Start: 2025-03-17 00:00:00+00:00; End: 2025-04-17 00:00:00+00:00
    # WINDOW & 1 WEEK STEP:       Start: 2025-02-24 00:00:00+00:00; End: 2025-03-24 00:00:00+00:00
    # WINDOW & 1 WEEK STEP MS:    Start: 2025-02-24 14:37:52+00:00; End: 2025-03-24 14:37:52+00:00
    # WINDOW & 2 WEEK STEP:       Start: 2025-02-28 00:00:00+00:00; End: 2025-03-31 00:00:00+00:00
    # WINDOW & 2 WEEK STEP MS:    Start: 2025-02-28 14:37:52+00:00; End: 2025-03-31 14:37:52+00:00
    #
    # WINDOW ONLY:                Start: 2025-04-17 00:00:00+00:00; End: 2025-05-17 00:00:00+00:00
    # WINDOW & 1 WEEK STEP:       Start: 2025-02-28 00:00:00+00:00; End: 2025-03-31 00:00:00+00:00
    # WINDOW & 1 WEEK STEP MS:    Start: 2025-02-28 14:37:52+00:00; End: 2025-03-31 14:37:52+00:00
    # WINDOW & 2 WEEK STEP:       Start: 2025-03-14 00:00:00+00:00; End: 2025-04-14 00:00:00+00:00
    # WINDOW & 2 WEEK STEP MS:    Start: 2025-03-14 14:37:52+00:00; End: 2025-04-14 14:37:52+00:00
