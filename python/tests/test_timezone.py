from raphtory import Graph
from datetime import datetime, timezone
import pytz

utc = timezone.utc


def test_datetime_with_timezone():
    g = Graph()
    # testing zones east and west of UK
    timezones = [
        "Asia/Kolkata",
        "America/New_York",
        "US/Central",
        "Europe/London",
        "Australia/Sydney",
        "Africa/Johannesburg",
    ]
    results = [
        datetime(2024, 1, 5, 1, 0, tzinfo=utc),
        datetime(2024, 1, 5, 6, 30, tzinfo=utc),
        datetime(2024, 1, 5, 10, 0, tzinfo=utc),
        datetime(2024, 1, 5, 12, 0, tzinfo=utc),
        datetime(2024, 1, 5, 17, 0, tzinfo=utc),
        datetime(2024, 1, 5, 18, 0, tzinfo=utc),
    ]

    for tz in timezones:
        timezone = pytz.timezone(tz)
        naive_datetime = datetime(2024, 1, 5, 12, 0, 0)
        localized_datetime = timezone.localize(naive_datetime)
        g.add_node(localized_datetime, 1)

    # @with_disk_graph FIXME: need special handling for nodes additions from Graph
    def check(g):
        assert g.node(1).history_date_time() == results

    check(g)
