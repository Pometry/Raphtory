from datetime import datetime
from typing import Union, Literal

Prop = Union[
    bool,
    int,
    datetime,
    str,
    "Graph",
    "PersistentGraph",
    "Document",
    list["Prop"],
    dict[str, "Prop"],
]

Direction = Literal["in", "out", "both"]

InputNode = Union[int, str, "Node"]

TimeInput = Union[int, str, datetime]
