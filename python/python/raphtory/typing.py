from datetime import datetime, date
from typing import Union, Literal, Mapping
import raphtory

PropValue = Union[
    bool,
    int,
    float,
    datetime,
    str,
    "Document",
    list["PropValue"],
    dict[str, "PropValue"],
]

GID = Union[int, str]

PropInput = Mapping[str, PropValue]

Direction = Literal["in", "out", "both"]

NodeInput = Union[int, str, "Node"]

TimeInput = Union[int, str, float, datetime, date, raphtory.EventTime, raphtory.OptionalEventTime]
