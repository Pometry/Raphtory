"""Python wrappers for the table api"""

from pyraphtory.interop import GenericScalaProxy, register, logger, to_jvm
import pandas as pd
import datetime as dt


@register(name="ProgressTracker")
class ProgressTracker(GenericScalaProxy):
    _classname = "com.raphtory.api.progresstracker.ProgressTracker"

    def inner_tracker(self):
        logger.trace("Progress tracker inner tracker returned")
        return to_jvm(self)


@register(name="Table")
class Table(GenericScalaProxy):
    _classname = "com.raphtory.api.analysis.table.Table"

    def to_df(self):
        rows = []
        columns = ()
        time_formatted=False
        for res in self.get():
            columns = res.actual_header()
            window = res.perspective().window()
            timestamp = res.perspective().formatted_time()
            time_formatted = res.perspective().format_as_date()
            if window != None:
                columns = ('timestamp', 'window', *columns)
                for r in res.rows():
                    window_size = window.get().output()
                    rows.append((timestamp, window_size, *r.values()))
            else:
                columns = ('timestamp', *columns)
                for r in res.rows():
                    rows.append((timestamp, *r.values()))

        df = pd.DataFrame.from_records(rows, columns=columns)
        if time_formatted:
            df["timestamp"] = df["timestamp"].apply(lambda x: dt.datetime.strptime(x,"%Y-%m-%d %H:%M:%S.%f"))
            return df
        else:
            return df