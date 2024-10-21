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
    list["Prop"],
    dict[str, "Prop"],
]

PropInput = Mapping[str, PropValue]

Direction = Literal["in", "out", "both"]

InputNode = Union[int, str, "Node"]

TimeInput = Union[int, str, float, datetime]
