from datetime import datetime
from typing import Union, Literal, Mapping


PropValue = Union[
    bool,
    int,
    float,
    datetime,
    str,
    "Graph",
    "PersistentGraph",
    "Document",
    list["PropValue"],
    dict[str, "PropValue"],
]

GID = Union[int, str]

PropInput = Mapping[str, PropValue]

Direction = Literal["in", "out", "both"]

NodeInput = Union[int, str, "Node"]

TimeInput = Union[int, str, float, datetime]
